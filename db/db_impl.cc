// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

// TableCache 以外的其他用途（如日志文件、MANIFEST 文件等）保留的文件数量。
const int kNumNonTableCacheFiles = 10;

// 为每个等待的写入者保存的信息
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu) // 构造函数，mu 用于初始化条件变量 cv
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;          // 此写入操作的状态
  WriteBatch* batch;      // 要写入的数据批次
  bool sync;              // 是否需要同步写入（持久化到磁盘）
  bool done;              // 此写入操作是否已完成
  port::CondVar cv;       // 用于等待此写入操作完成的条件变量
};

// Compaction 过程中的状态信息
struct DBImpl::CompactionState {
  // Compaction 产生的文件信息
  struct Output {
    uint64_t number;         // 文件编号
    uint64_t file_size;      // 文件大小
    InternalKey smallest, largest; // 文件中最小和最大的内部键
  };

  // 获取当前正在生成的输出文件信息
  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c) // 构造函数
      : compaction(c),
        smallest_snapshot(0), // Compaction 期间需要考虑的最旧快照序列号
        outfile(nullptr),     // 当前正在写入的输出文件
        builder(nullptr),     // 用于构建 SSTable 的 TableBuilder
        total_bytes(0) {}     // 本次 Compaction 总共写入的字节数

  Compaction* const compaction; // 指向描述此次 Compaction 信息的 Compaction 对象

  // 小于 smallest_snapshot 的序列号是不重要的，因为我们永远不需要服务于
  // smallest_snapshot 以下的快照。因此，如果我们看到了一个序列号 S <= smallest_snapshot，
  // 我们可以丢弃所有具有相同键且序列号 < S 的条目。
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;  // 一次 SSTable 合并可能会写出多个新的 SSTable

  // 为正在生成的输出文件保存的状态
  WritableFile* outfile;    // 指向当前输出文件的指针
  TableBuilder* builder;    // 指向当前 TableBuilder 的指针

  uint64_t total_bytes;     // 本次 Compaction 已写入的总字节数
};

// 将用户提供的选项值限制在合理的范围内
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

// 规范化用户提供的 Options，确保其值在合理范围内，并设置内部使用的比较器和过滤器策略。
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src; // 复制用户提供的原始选项
  result.comparator = icmp; // 设置内部比较器
  // 如果用户提供了过滤器策略，则使用内部过滤器策略；否则为 nullptr
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;

  // 限制各种选项参数的范围
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000); // 最大打开文件数
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30); // 写缓冲区大小 (64KB - 1GB)
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);     // 单个 SSTable 文件最大大小 (1MB - 1GB)
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);      // 数据块大小 (1KB - 4MB)

  // 如果用户没有提供 info_log，则在数据库目录中创建一个默认的日志文件
  if (result.info_log == nullptr) {
    src.env->CreateDir(dbname);  // 确保数据库目录存在
    // 重命名旧的 INFO_LOG 文件（如果存在）
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log); // 创建新的日志记录器
    if (!s.ok()) {
      // 如果创建日志记录器失败，则不记录日志
      result.info_log = nullptr;
    }
  }
  // 如果用户没有提供 block_cache，则创建一个默认大小为 8MB 的 LRU 缓存
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result; // 返回规范化后的 Options
}

// 计算 TableCache 的大小
static int TableCacheSize(const Options& sanitized_options) {
  // 为其他用途（如 MANIFEST, log 文件等）保留 kNumNonTableCacheFiles 个文件描述符，
  // 其余的分配给 TableCache。
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// DBImpl 类的构造函数
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env), // 初始化环境对象，来自用户提供的原始选项
      internal_comparator_(raw_options.comparator), // 初始化内部比较器，使用用户提供的比较器
      internal_filter_policy_(raw_options.filter_policy), // 初始化内部过滤器策略，使用用户提供的过滤器策略
      options_(SanitizeOptions(dbname, &internal_comparator_, // 对用户提供的选项进行规范化处理，生成内部使用的选项
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log), // 判断 DBImpl 是否拥有 info_log 的所有权（即是否是内部创建的）
      owns_cache_(options_.block_cache != raw_options.block_cache), // 判断 DBImpl 是否拥有 block_cache 的所有权
      dbname_(dbname), // 数据库名称
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))), // 初始化 TableCache，用于缓存 SSTable
      db_lock_(nullptr), // 数据库锁文件句柄，初始为空
      shutting_down_(false), // 数据库是否正在关闭的标志，初始为 false
      background_work_finished_signal_(&mutex_), // 用于后台工作完成的条件变量
      mem_(nullptr), // 当前的可写 MemTable，初始为空
      imm_(nullptr), // 不可变的 MemTable (等待被 Compact)，初始为空
      has_imm_(false), // 是否存在不可变的 MemTable 的标志
      logfile_(nullptr), // 当前日志文件的可写文件对象，初始为空
      logfile_number_(0), // 当前日志文件的编号，初始为 0
      log_(nullptr), // 日志写入器对象，初始为空
      seed_(0), // 用于生成迭代器 ID 的种子
      tmp_batch_(new WriteBatch), // 用于合并多个写入操作的临时 WriteBatch
      background_compaction_scheduled_(false), // 后台 Compaction 是否已调度的标志
      manual_compaction_(nullptr), // 指向手动 Compaction 请求的指针
      versions_(new VersionSet(dbname_, &options_, table_cache_, // 初始化 VersionSet，管理数据库的版本信息
                               &internal_comparator_)) {}

// DBImpl 类的析构函数
DBImpl::~DBImpl() {
  // 等待后台工作完成
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release); // 设置正在关闭的标志
  while (background_compaction_scheduled_) { // 等待已调度的后台 Compaction 完成
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  // 释放数据库锁文件
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  // 释放各种资源
  delete versions_; // 删除 VersionSet
  if (mem_ != nullptr) mem_->Unref(); // 解引用当前 MemTable
  if (imm_ != nullptr) imm_->Unref(); // 解引用不可变 MemTable
  delete tmp_batch_; // 删除临时 WriteBatch
  delete log_; // 删除日志写入器
  delete logfile_; // 删除日志文件对象
  delete table_cache_; // 删除 TableCache

  // 如果 DBImpl 拥有 info_log 或 block_cache 的所有权，则释放它们
  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

// 创建一个新的数据库（初始化 MANIFEST 文件和 CURRENT 文件）
Status DBImpl::NewDB() {
  VersionEdit new_db; // 创建一个新的 VersionEdit 用于初始化数据库元信息
  new_db.SetComparatorName(user_comparator()->Name()); // 设置比较器名称
  new_db.SetLogNumber(0); // 初始日志文件编号，表示所有编号 >= 0 的日志文件都未被 compact
  new_db.SetNextFile(2);  // 下一个可用的文件编号从 2 开始 (1 用于 MANIFEST)
  new_db.SetLastSequence(0); // 最后的序列号初始化为 0

  const std::string manifest = DescriptorFileName(dbname_, 1); // MANIFEST 文件名 (dbname_/MANIFEST-000001)
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file); // 创建 MANIFEST 文件
  if (!s.ok()) {
    return s;
  }
  { // 将 VersionEdit 写入 MANIFEST 文件进行初始化
    log::Writer log(file); // 使用 log::Writer 格式化写入
    std::string record;
    new_db.EncodeTo(&record); // 将 VersionEdit 编码为字符串
    s = log.AddRecord(record); // 将记录添加到 MANIFEST 文件
    if (s.ok()) {
      s = file->Sync(); // 同步到磁盘
    }
    if (s.ok()) {
      s = file->Close(); // 关闭文件
    }
  }
  delete file;
  if (s.ok()) {
    // 创建 CURRENT 文件，指向新的 MANIFEST 文件
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest); // 如果出错，删除创建的 MANIFEST 文件
  }
  return s;
}

// 根据 paranoid_checks 选项决定是否忽略错误
void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // 如果状态正常，或者启用了偏执检查，则不改变状态
  } else {
    // 否则，记录错误信息并将其标记为 OK (忽略错误)
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

// 移除过时的文件（如旧的日志文件、不再被任何版本引用的 SSTable 文件等）
void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁

  if (!bg_error_.ok()) {
    // 如果发生后台错误，我们不知道新版本是否已提交，
    // 因此不能安全地进行垃圾回收。
    return;
  }

  // 构建一个包含所有活动文件编号的集合
  std::set<uint64_t> live = pending_outputs_; // pending_outputs_ 包含正在写入的临时文件或 compaction 输出文件
  versions_->AddLiveFiles(&live); // 将当前所有 Version 中引用的 SSTable 文件编号加入 live 集合

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // 获取数据库目录下的所有文件名（有意忽略错误）
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete; // 存储待删除的文件名
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) { // 解析文件名获取文件编号和类型
      bool keep = true; // 默认保留文件
      switch (type) {
        case kLogFile: // 日志文件
          // 保留当前日志文件、前一个日志文件（用于恢复）以及所有比当前日志文件更新的日志文件
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile: // MANIFEST 文件
          // 保留当前的 MANIFEST 文件以及任何更新的 MANIFEST 文件
          // （以防存在允许其他实例的竞争情况）
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile: // SSTable 文件
          // 仅保留在 live 集合中（即被当前版本或正在进行的 compaction 引用的文件）
          keep = (live.find(number) != live.end());
          break;
        case kTempFile: // 临时文件
          // 任何当前正在写入的临时文件都必须记录在 pending_outputs_ 中，
          // pending_outputs_ 已插入到 "live" 集合中
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:    // CURRENT 文件
        case kDBLockFile:     // 数据库锁文件
        case kInfoLogFile:    // INFO_LOG 文件
          keep = true; // 这些文件总是保留
          break;
      }

      if (!keep) { // 如果文件不需要保留
        files_to_delete.push_back(std::move(filename)); // 加入待删除列表
        if (type == kTableFile) {
          table_cache_->Evict(number); // 从 TableCache 中移除对应的 SSTable
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // 在删除所有文件时，解锁其他线程。所有正在删除的文件
  // 都具有唯一的名称，不会与新创建的文件冲突，
  // 因此在允许其他线程继续进行的同时删除是安全的。
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename); // 删除文件
  }
  mutex_.Lock(); // 重新获取锁
}

// 数据库恢复过程
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁

  // 忽略 CreateDir 的错误，因为数据库的创建仅在描述符创建时才提交，
  // 并且此目录可能已因先前的失败创建尝试而存在。
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_); // 获取数据库锁文件
  if (!s.ok()) {
    return s;
  }

  // 检查 CURRENT 文件是否存在，以判断数据库是否已存在
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) { // 如果选项允许且数据库不存在，则创建新数据库
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();  // 调用 NewDB 初始化数据库（创建 MANIFEST 和 CURRENT）
      if (!s.ok()) {
        return s;
      }
    } else { // 否则返回错误
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else { // 如果数据库已存在
    if (options_.error_if_exists) { // 如果选项要求在数据库已存在时报错，则返回错误
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  // 1. 首先从 MANIFEST 文件恢复 VersionSet
  s = versions_->Recover(save_manifest);  // 恢复 MANIFEST 到 VersionSet
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0); // 用于记录恢复过程中遇到的最大序列号

  // 2. 然后重做日志文件，过程中序列号和文件编号都会得到更新
  // 从所有比描述符中指定的日志文件更新的日志文件中恢复
  // （上一个实例可能在未在描述符中注册它们的情况下添加了新的日志文件）。
  //
  // 注意 PrevLogNumber() 不再使用，但我们关注它，以防我们正在恢复
  // 由旧版本 leveldb 生成的数据库。
  const uint64_t min_log = versions_->LogNumber(); // MANIFEST 中记录的当前日志文件编号
  const uint64_t prev_log = versions_->PrevLogNumber(); // MANIFEST 中记录的前一个日志文件编号（兼容旧版）
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames); // 获取数据库目录下的所有文件
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;  // 期望存在的所有 SSTable 文件编号
  versions_->AddLiveFiles(&expected); // 从 VersionSet 获取所有活动的 SSTable 文件编号
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs; // 收集需要用于 MemTable 宕机恢复的日志文件编号
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number); // 从期望列表中移除实际存在的文件
      if (type == kLogFile && ((number >= min_log) || (number == prev_log))) {
        // 如果是日志文件，并且其编号大于等于 min_log 或等于 prev_log，则加入待恢复列表
        logs.push_back(number);
      }
    }
  }
  if (!expected.empty()) {  // 如果期望的 SSTable 文件有缺失
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin()))); // 报告SST文件缺失错误
  }

  // 按日志文件生成的顺序进行恢复
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    // 重做日志文件，并将变更记录到 edit 中，同时更新 max_sequence
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // 上一个实例可能在分配此日志号后没有写入任何 MANIFEST 记录。
    // 因此，我们手动更新 VersionSet 中的文件号分配计数器。
    versions_->MarkFileNumberUsed(logs[i]); // 使用日志文件编号更新 VersionSet 的最新文件编号。
    // 分析：【SST 间 merge 时 MANIFEST 写入的是 mem_ 的 log id+1 作为 next file num，之后 mem_ 给了 imm_，mem_ 开了新的 log，这种情况 MANIFEST 就不是最新 file num】
    // 重启 DB 后，重新生成 MANIFEST 时用的 file num 其实会和之前 mem_ 新开的那个 log num 相同，这个时序我猜是会发生的。
  }

  // 更新 VersionSet 的最后序列号
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

// 从指定的日志文件恢复数据
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  // 用于报告日志读取错误的辅助结构体
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // 如果 options_.paranoid_checks==false 则为 null
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s; // 如果需要，记录错误状态
    }
  };

  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁

  // 打开日志文件
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file); // 以顺序读取方式打开文件
  if (!status.ok()) {
    MaybeIgnoreError(&status); // 根据选项决定是否忽略错误
    return status;
  }

  // 创建日志读取器
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr); // 如果启用偏执检查，则传递 status 指针以记录错误
  // 我们有意让 log::Reader 进行校验和检查，即使 paranoid_checks==false，
  // 以便损坏导致整个提交被跳过，而不是传播错误信息（如过大的序列号）。
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/); // true 表示进行校验和检查
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // 读取所有记录并添加到 MemTable 中
  std::string scratch; // 用于 ReadRecord 的临时空间
  Slice record;
  WriteBatch batch;
  int compactions = 0; // 记录在恢复此日志文件期间发生的 MemTable Compaction 次数
  MemTable* mem = nullptr; // 用于恢复日志记录的 MemTable
  while (reader.ReadRecord(&record, &scratch) && status.ok()) { // 循环读取日志记录
    if (record.size() < 12) { // 日志记录头部至少包含序列号和数量 (8+4=12字节)
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record); // 将日志记录内容设置到 WriteBatch 中

    if (mem == nullptr) { // 如果 MemTable 未创建，则创建一个新的
      mem = new MemTable(internal_comparator_);
      mem->Ref(); // 增加引用计数
    }
    status = WriteBatchInternal::InsertInto(&batch, mem); // 将 WriteBatch 中的操作插入到 MemTable
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    // 更新遇到的最大序列号
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    // 如果 MemTable 的内存使用量超过了写缓冲区大小，则将其 Compact 到 Level-0
    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true; // 标记需要保存 MANIFEST (因为有新的 SSTable 生成)
      status = WriteLevel0Table(mem, edit, nullptr);  // 将 MemTable 内容写入 Level-0 SSTable
      mem->Unref(); // MemTable 内容已写入文件，解引用
      mem = nullptr;
      if (!status.ok()) {
        // 立即反映错误，以便像文件系统已满这样的情况导致 DB::Open() 失败。
        break;
      }
    }
  }

  delete file; // 关闭并删除日志文件对象

  // 查看是否应该继续重用最后一个日志文件。
  // 条件：状态正常，选项允许重用日志，这是最后一个日志文件，并且恢复期间没有发生 MemTable Compaction。
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    // 获取旧日志文件的大小，并尝试以追加模式打开它
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size); // 创建新的日志写入器，从文件末尾开始写
      logfile_number_ = log_number; // 设置当前日志文件编号
      if (mem != nullptr) { // 如果恢复过程中创建的 MemTable 未被 Compact
        mem_ = mem; // 将其作为当前的活动 MemTable
        mem = nullptr;
      } else {
        // 如果日志文件存在但为空，mem 可能为 nullptr。
        mem_ = new MemTable(internal_comparator_); // 创建一个新的空 MemTable
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) { // 如果恢复日志过程中 MemTable 未被重用或 Compact
    // MemTable 没有被重用；将其 Compact。
    if (status.ok()) {
      *save_manifest = true; // 标记需要保存 MANIFEST
      status = WriteLevel0Table(mem, edit, nullptr); // 将其内容写入 Level-0 SSTable
    }
    mem->Unref(); // 解引用
  }

  return status;
}

// 将 MemTable 的内容写入 Level-0 的 SSTable 文件
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  const uint64_t start_micros = env_->NowMicros(); // 记录开始时间
  FileMetaData meta; // 用于存储新生成的 SSTable 的元数据
  meta.number = versions_->NewFileNumber(); // 获取一个新的文件编号
  pending_outputs_.insert(meta.number); // 将文件编号加入正在输出的集合，防止被清理
  Iterator* iter = mem->NewIterator(); // 获取 MemTable 的迭代器
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock(); // 在构建 SSTable 期间释放锁，因为 MemTable 是不可变的
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta); // 调用 BuildTable 构建 SSTable
    mutex_.Lock(); // 重新获取锁
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter; // 删除迭代器
  pending_outputs_.erase(meta.number); // 从正在输出的集合中移除文件编号

  // 注意：如果 file_size 为零，则文件已被删除，不应添加到 MANIFEST 中。
  int level = 0; // 默认写入 Level-0
  if (s.ok() && meta.file_size > 0) { // 如果构建成功且文件大小大于0
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) { // 如果提供了基础版本 (通常用于 Minor Compaction)
      // 根据新 SSTable 的键范围，尝试将其直接推送到更深的 Level (如果与该 Level 没有重叠)
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    // 将新生成的 SSTable 文件信息添加到 VersionEdit 中
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  // 记录 Compaction 统计信息
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros; // 计算耗时
  stats.bytes_written = meta.file_size; // 记录写入字节数
  stats_[level].Add(stats); // 将统计信息添加到对应 Level
  return s;
}

// 将不可变的 MemTable (imm_) Compact 成 SSTable 文件
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  assert(imm_ != nullptr); // 确保存在不可变的 MemTable

  // 将 MemTable 的内容保存为一个新的 Table (SSTable)
  VersionEdit edit;
  Version* base = versions_->current(); // 获取当前版本作为基础
  base->Ref(); // 增加引用计数
  Status s = WriteLevel0Table(imm_, &edit, base); // 将 imm_ 写入 SSTable
  base->Unref(); // 解引用基础版本

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) { // 如果成功但数据库正在关闭
    s = Status::IOError("Deleting DB during memtable compaction"); // 返回 IO 错误
  }

  // 用生成的 Table 替换不可变的 MemTable
  if (s.ok()) {
    edit.SetPrevLogNumber(0); // 不再需要更早的日志文件
    edit.SetLogNumber(logfile_number_);  // 设置当前日志文件编号
    s = versions_->LogAndApply(&edit, &mutex_); // 将 VersionEdit 应用到 VersionSet 并记录到 MANIFEST
  }

  if (s.ok()) {
    // 提交到新状态
    imm_->Unref(); // 解引用旧的不可变 MemTable
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release); // 清除存在不可变 MemTable 的标志
    RemoveObsoleteFiles(); // 移除过时的文件
  } else {
    RecordBackgroundError(s); // 如果出错，记录后台错误
  }
}

// 手动触发对指定键范围的 Compaction (主要用于测试或特殊维护)
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1; // 初始化包含文件的最大层级
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    // 查找与指定范围有重叠的最高层级
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // 首先尝试 Compact MemTable (如果 MemTable 与范围重叠)
  // 从 Level-0 开始，逐层对与指定范围有重叠的层级进行 Compaction
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

// 测试接口：对指定 Level 和键范围进行 Compaction
void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels); // 确保 level+1 是有效层级

  InternalKey begin_storage, end_storage; // 用于存储转换后的内部键

  ManualCompaction manual; // 创建手动 Compaction 请求对象
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) { // 如果起始键为空，则表示从该层的开头开始
    manual.begin = nullptr;
  } else {
    // 将用户键转换为内部键（使用最大序列号和 kValueTypeForSeek 类型，确保能找到所有相关版本）
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) { // 如果结束键为空，则表示到该层的末尾结束
    manual.end = nullptr;
  } else {
    // 将用户键转换为内部键（使用序列号 0 和类型 0，确保能找到所有相关版本）
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  // 循环等待，直到手动 Compaction 完成、数据库关闭或发生后台错误
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // 如果当前没有手动 Compaction 任务
      manual_compaction_ = &manual; // 设置当前手动 Compaction 任务
      MaybeScheduleCompaction(); // 尝试调度 Compaction
    } else {  // 如果当前正在运行其他 Compaction (可能是本次的，也可能是其他的)
      background_work_finished_signal_.Wait(); // 等待后台工作完成信号
    }
  }
  if (manual_compaction_ == &manual) {
    // 如果由于某种原因提前中止，取消本次手动 Compaction
    manual_compaction_ = nullptr;
  }
}

// 测试接口：强制 Compact MemTable 并等待其完成
Status DBImpl::TEST_CompactMemTable() {
  // 传入 nullptr 表示只是等待之前的写入完成，并确保 MemTable (如果需要) 被置换为 imm_
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // 等待直到 Compaction 完成 (即 imm_ 变为 nullptr)
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) { // 如果 imm_ 仍然存在，说明发生了后台错误
      s = bg_error_;
    }
  }
  return s;
}

// 记录后台发生的错误
void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  if (bg_error_.ok()) { // 只记录第一个发生的后台错误
    bg_error_ = s;
    background_work_finished_signal_.SignalAll(); // 通知所有等待后台工作的线程
  }
}

// 尝试调度一次后台 Compaction
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  if (background_compaction_scheduled_) {
    // 如果已经调度过了，则不做任何事
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // 如果数据库正在关闭，则不再进行后台 Compaction
  } else if (!bg_error_.ok()) {
    // 如果已经发生后台错误，则不再进行任何更改
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // 如果没有不可变的 MemTable，没有手动 Compaction 请求，并且 VersionSet 认为不需要 Compaction，
    // 则没有工作可做。
  } else {
    // 否则，标记后台 Compaction 已调度，并通过环境调度 BGWork 函数执行
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

// 后台工作线程的入口函数 (静态成员函数，通过 env_->Schedule 调用)
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall(); // 将参数转换为 DBImpl* 并调用 BackgroundCall
}

// 后台 Compaction 的主要逻辑调用
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_); // 获取互斥锁
  assert(background_compaction_scheduled_); // 确保确实是已调度的 Compaction
  if (shutting_down_.load(std::memory_order_acquire)) {
    // 如果数据库正在关闭，则不进行后台工作
  } else if (!bg_error_.ok()) {
    // 如果发生后台错误，则不进行后台工作
  } else {
    BackgroundCompaction(); // 执行后台 Compaction
  }

  background_compaction_scheduled_ = false; // 清除已调度标志

  // 上一次 Compaction 可能在某个层级产生了过多的文件，
  // 因此如果需要，重新调度另一次 Compaction。
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll(); // 通知所有等待后台工作完成的线程
}

// 执行后台 Compaction 的核心逻辑
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁

  // 优先处理不可变的 MemTable (imm_) 的 Compaction
  if (imm_ != nullptr) {
    CompactMemTable(); // 将 imm_ Compact 到 SSTable
    return; // 完成 MemTable Compaction 后直接返回
  }

  Compaction* c; // 指向 Compaction 对象的指针
  bool is_manual = (manual_compaction_ != nullptr); // 是否是手动 Compaction
  InternalKey manual_end; // 用于手动 Compaction，记录本次 Compaction 的结束键
  if (is_manual) {  // 如果是手动 Compaction (通常用于测试或特定维护)
    ManualCompaction* m = manual_compaction_;
    // 从 VersionSet 获取手动指定范围的 Compaction 对象
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr); // 如果 c 为空，表示该范围没有可 Compaction 的内容，任务完成
    if (c != nullptr) {
      // 记录手动 Compaction 输入文件的最大键，用于下次迭代 (如果本次未完成整个范围)
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else { // 自动 Compaction
    c = versions_->PickCompaction();  // 根据当前 VersionSet 的状态选择一个最合适的 Compaction 任务
  }

  Status status;
  if (c == nullptr) {
    // 如果没有 Compaction 任务 (c 为空)，则什么也不做
  } else if (!is_manual && c->IsTrivialMove()) {  // 如果不是手动 Compaction，并且是平凡移动
    // 平凡移动：Level-i 只有一个 SSTable，并且它与 Level-(i+1) 的 SSTable 没有任何键范围重叠，
    // 此时可以直接将该 SSTable 移动到 Level-(i+1)。
    assert(c->num_input_files(0) == 1); // 确保输入文件只有一个
    FileMetaData* f = c->input(0, 0); // 获取该输入文件的元数据
    c->edit()->RemoveFile(c->level(), f->number); // 从 VersionEdit 中移除 Level-i 的该文件
    // 将该文件添加到 VersionEdit 的 Level-(i+1)
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);  // 应用 VersionEdit，这里只是元数据变更，没有实际的 SSTable I/O 操作
    if (!status.ok()) {
      RecordBackgroundError(status); // 如果出错，记录后台错误
    }
    VersionSet::LevelSummaryStorage tmp; // 用于存储 Level 概要信息
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {  // 如果 SSTable 和下一层 SSTable 有重叠，或者有多个输入文件，需要进行合并 (Merge)
    CompactionState* compact = new CompactionState(c); // 创建 CompactionState 对象
    status = DoCompactionWork(compact); // 执行实际的 SSTable 合并工作，这里会发生 I/O
                                        // I/O 过程中会临时释放锁，因为被 Compact 的 SSTable 都是不可变的
    if (!status.ok()) {
      RecordBackgroundError(status); // 如果出错，记录后台错误
    }
    CleanupCompaction(compact); // 清理 CompactionState 相关资源
    c->ReleaseInputs(); // 释放 Compaction 输入文件的引用
    RemoveObsoleteFiles(); // 移除过时的文件
  }
  delete c; // 删除 Compaction 对象

  if (status.ok()) {
    // Compaction 完成
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // 如果在关闭过程中发现 Compaction 错误，则忽略
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) { // 如果是手动 Compaction
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) { // 如果出错，标记手动 Compaction 完成
      m->done = true;
    }
    if (!m->done) { // 如果手动 Compaction 未完成 (例如只 Compact 了请求范围的一部分)
      // 更新手动 Compaction 请求的起始键，以便下次继续
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr; // 清除当前手动 Compaction 请求
  }
}

// 清理 CompactionState 相关资源
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  if (compact->builder != nullptr) { // 如果 TableBuilder 存在
    // 可能在 Compaction 中途收到关闭调用
    compact->builder->Abandon(); // 放弃未完成的 TableBuilder
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr); // 如果 builder 为空，则 outfile 也应为空
  }
  delete compact->outfile; // 删除输出文件对象
  // 从 pending_outputs_ 集合中移除本次 Compaction 生成的所有文件编号
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact; // 删除 CompactionState 对象
}

// 为 Compaction 创建一个新的输出 SSTable 空文件
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr); // 确保当前没有 TableBuilder
  uint64_t file_number;
  {
    mutex_.Lock(); // 获取锁以分配新的文件编号
    file_number = versions_->NewFileNumber(); // 从 VersionSet 获取新的文件编号
    pending_outputs_.insert(file_number); // 将新文件编号加入正在写入的 SSTable 标识集合，避免被垃圾文件清理带走
    CompactionState::Output out; // 创建新的输出文件信息
    out.number = file_number;
    out.smallest.Clear(); // 初始化最小键
    out.largest.Clear();  // 初始化最大键
    compact->outputs.push_back(out); // 加入 CompactionState 的输出列表
    mutex_.Unlock(); // 释放锁
  }

  // 创建输出文件
  std::string fname = TableFileName(dbname_, file_number); // 生成 SSTable 文件名
  Status s = env_->NewWritableFile(fname, &compact->outfile); // 创建可写文件对象
  if (s.ok()) { // 如果文件创建成功
    compact->builder = new TableBuilder(options_, compact->outfile); // 创建 TableBuilder 用于构建 SSTable
  }
  return s;
}

// 完成一个 Compaction 输出 SSTable 文件的写入
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr); // 确保输出文件对象存在
  assert(compact->builder != nullptr); // 确保 TableBuilder 存在

  const uint64_t output_number = compact->current_output()->number; // 获取当前输出文件的编号
  assert(output_number != 0);

  // 检查输入迭代器的状态
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries(); // 获取已写入 TableBuilder 的条目数
  if (s.ok()) { // 如果迭代器状态正常
    s = compact->builder->Finish(); // 完成 TableBuilder 的构建过程（写入元数据块、索引块等）
  } else { // 如果迭代器出错
    compact->builder->Abandon(); // 放弃 TableBuilder，不生成有效的 SSTable
  }
  const uint64_t current_bytes = compact->builder->FileSize(); // 获取生成的文件大小
  compact->current_output()->file_size = current_bytes; // 更新 CompactionState 中该输出文件的元数据
  compact->total_bytes += current_bytes; // 累加总写入字节数
  delete compact->builder; // 删除 TableBuilder
  compact->builder = nullptr;

  // 完成并检查文件错误
  if (s.ok()) {
    s = compact->outfile->Sync(); // 同步文件内容到磁盘
  }
  if (s.ok()) {
    s = compact->outfile->Close();  // 关闭 SSTable 文件
  }
  delete compact->outfile; // 删除文件对象
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) { // 如果一切正常且文件中有条目
    // 验证生成的 SSTable 是否可用（通过尝试创建一个迭代器）
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status(); // 检查迭代器状态
    delete iter;
    if (s.ok()) { // 如果 SSTable 可用
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(), // 记录生成的 SSTable 信息
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

// 将 Compaction 的结果（新生成的 SSTable 和被删除的旧 SSTable）应用到 VersionSet
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  // 记录 Compaction 的概要信息
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(), // Level-i 的输入文件数和层级
      compact->compaction->num_input_files(1), compact->compaction->level() + 1, // Level-(i+1) 的输入文件数和层级
      static_cast<long long>(compact->total_bytes)); // Compaction 生成的总字节数

  // 添加 Compaction 的输出（新生成的 SSTable）
  // 1. 将被合并的旧 SSTable 标记为删除，记录到 VersionEdit 中
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level(); // Compaction 发生的较低层级 (Level-i)
  // 2. 将合并产生的新 SSTable 添加到 VersionEdit 中 (通常添加到 Level-(i+1))
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  // 将 VersionEdit 应用到 VersionSet，生成新的当前 Version，并记录到 MANIFEST
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

// 执行 Compaction 的实际工作（合并 SSTable）
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros(); // 记录开始时间
  int64_t imm_micros = 0;  // 用于记录在 SSTable 合并期间，穿插进行的 imm_ Compaction 所花费的时间

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(), // Level-i 的输入文件数和层级
      compact->compaction->num_input_files(1), // Level-(i+1) 的输入文件数
      compact->compaction->level() + 1); // Level-(i+1)

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0); // 确保 Compaction 的输入层至少有一个文件
  assert(compact->builder == nullptr); // 确保 CompactionState 的 builder 初始为空
  assert(compact->outfile == nullptr); // 确保 CompactionState 的 outfile 初始为空

  // 确定 smallest_snapshot，用于判断哪些旧版本的 key 可以被丢弃
  if (snapshots_.empty()) { // 如果当前没有活动的快照
    compact->smallest_snapshot = versions_->LastSequence(); // 使用最新的序列号
  } else { // 如果有活动的快照
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();  // 使用最旧的快照的序列号
  }

  // 创建一个合并迭代器，用于遍历所有输入 SSTable 的数据
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // 在实际进行 Compaction 工作时释放互斥锁，因为被 Compact 的 SSTable 是不可变的，
  // current Version 的最终引用在 VersionSet 手里。
  mutex_.Unlock();

  // 开始合并多个 SSTable
  input->SeekToFirst(); // 定位到输入数据的第一个条目
  Status status;
  ParsedInternalKey ikey; // 用于解析内部键
  std::string current_user_key; // 当前正在处理的用户键
  bool has_current_user_key = false; // 是否已记录当前用户键
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;  // 当前用户键的上一个遇到的序列号
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) { // 循环遍历输入数据，直到无效或数据库关闭
    // 优先处理不可变 MemTable (imm_) 的 Compaction 工作
    if (has_imm_.load(std::memory_order_relaxed)) { // 如果存在 imm_
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();          // 获取锁，避免 SSTable 合并期间 MemTable 打满，需要穿插进行 MemTable Compaction
      if (imm_ != nullptr) {
        CompactMemTable();  // 对 imm_ 进行 Compaction，不需要担心并发，因为 imm_ 也是不可变的，只有 Compaction 线程会修改 imm_ 字段
        // 如果有线程在 MakeRoomForWrite 中等待，唤醒它们
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start); // 累加 imm_ Compaction 的耗时
    }

    Slice key = input->key(); // 获取当前内部键
    // 如果当前键超出了本次 Compaction 输出文件的范围，并且当前 builder 不为空，
    // 则先完成当前的输出文件。
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break; // 如果出错，则停止 Compaction
      }
    }

    // 处理键/值，添加到状态等。
    bool drop = false; // 标记当前条目是否应该被丢弃
    if (!ParseInternalKey(key, &ikey)) { // 解析内部键
      // 不隐藏错误键（例如解析失败的键）
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else { // 内部键解析成功
      // 如果遇到新的用户键
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // 首次出现此用户键
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber; // 首次遇到用户键，没有上一个序列号
      }

      // 判断是否可以丢弃当前条目 (基于 MVCC 原则)
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // 如果当前用户键的上一个版本的序列号已经小于或等于 smallest_snapshot，
        // 意味着这个更旧的版本 (last_sequence_for_key) 对于所有活动快照和未来读取都是不可见的，
        // 因此当前这个版本 (ikey.sequence，必然小于 last_sequence_for_key) 也可以被安全丢弃。
        // 这是因为我们总是保留一个 user_key 的最新版本（或删除标记）直到其序列号小于 smallest_snapshot。
        drop = true;  // 规则 (A): 被同一用户键的更新条目所隐藏
      } else if (ikey.type == kTypeDeletion &&      // 如果当前条目是删除标记
                 ikey.sequence <= compact->smallest_snapshot && // 并且其序列号小于等于 smallest_snapshot
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) { // 并且该用户键在更底层级 (base level) 没有数据
        // 对于此用户键：
        // (1) 在更高层级没有数据 (因为我们是从高层向低层合并)
        // (2) 在更低层级的数据将具有更大的序列号 (LevelDB 的设计)
        // (3) 在当前正在合并的层级中，具有更小序列号的数据将在接下来的
        //     几次循环迭代中被丢弃 (根据上面的规则 A)。
        // 因此，这个删除标记是过时的，可以被丢弃。
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;  // 更新当前用户键的上一个遇到的序列号
    }
#if 0 // 用于调试的日志输出
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    // 如果当前条目不被丢弃，则将其添加到合并后的 SSTable 中
    if (!drop) {
      // 如果需要，打开输出文件
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break; // 如果打开文件失败，则停止 Compaction
        }
      }
      if (compact->builder->NumEntries() == 0) { // 如果是 SSTable 的第一个条目
        compact->current_output()->smallest.DecodeFrom(key);  // 记录 SSTable 写入的第一个键（也是最小的键）
      }
      compact->current_output()->largest.DecodeFrom(key); // 更新 SSTable 的最后一个键（也是最大的键）
      compact->builder->Add(key, input->value()); // 将键值对添加到 TableBuilder

      // 如果输出文件足够大，则关闭它并开始新的输出文件
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) { // SSTable 大小达到上限
        status = FinishCompactionOutputFile(compact, input);  // 完成当前 SSTable 文件
        if (!status.ok()) {
          break; // 如果出错，则停止 Compaction
        }
      }
    }

    input->Next(); // 处理下一个输入条目
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) { // 如果 Compaction 正常完成但数据库正在关闭
    status = Status::IOError("Deleting DB during compaction"); // 返回 IO 错误
  }
  if (status.ok() && compact->builder != nullptr) { // 如果 Compaction 正常完成且 builder 中还有数据
    status = FinishCompactionOutputFile(compact, input);  // 将 builder 中剩余的数据写入最后一个 SSTable 文件
  }
  if (status.ok()) { // 如果到目前为止都正常
    status = input->status(); // 获取输入迭代器的最终状态
  }
  delete input; // 删除输入迭代器
  input = nullptr;

  // 记录 Compaction 统计信息
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros; // 计算总耗时 (减去 imm_ Compaction 的时间)
  // 累加读取的字节数
  for (int which = 0; which < 2; which++) { // which=0 表示 level-i, which=1 表示 level-(i+1)
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  // 累加写入的字节数
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock(); // 获取锁以更新统计信息和应用 Compaction 结果
  stats_[compact->compaction->level() + 1].Add(stats); // 将统计信息添加到目标层级

  if (status.ok()) { // 如果 Compaction 工作本身没有错误
    status = InstallCompactionResults(compact); // 将 Compaction 结果应用到 VersionSet (即 VersionEdit 的 apply)
  }
  if (!status.ok()) { // 如果应用结果时出错或之前 Compaction 工作出错
    RecordBackgroundError(status); // 记录后台错误
  }
  VersionSet::LevelSummaryStorage tmp; // 用于存储 Level 概要信息
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp)); // 记录 Compaction 后的 Level 概要
  return status;
}

namespace { // 匿名命名空间

// 用于管理迭代器相关状态的结构体，主要用于注册清理函数
struct IterState {
  port::Mutex* const mu; // 指向 DBImpl 的互斥锁
  Version* const version GUARDED_BY(mu); // 迭代器关联的 Version
  MemTable* const mem GUARDED_BY(mu);    // 迭代器关联的当前 MemTable
  MemTable* const imm GUARDED_BY(mu);    // 迭代器关联的不可变 MemTable (可能为 nullptr)

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

// 迭代器的清理函数，在迭代器被销毁时调用，用于解引用相关资源
static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock(); // 获取锁
  state->mem->Unref(); // 解引用 MemTable
  if (state->imm != nullptr) state->imm->Unref(); // 如果存在，解引用不可变 MemTable
  state->version->Unref(); // 解引用 Version
  state->mu->Unlock(); // 释放锁
  delete state; // 删除 IterState 对象
}

}  // 匿名命名空间结束

// 创建一个内部迭代器，它合并了 MemTable、不可变 MemTable 和所有 SSTable 的内容
Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock(); // 获取锁
  *latest_snapshot = versions_->LastSequence(); // 获取当前最新的序列号作为快照

  // 收集所有需要的子迭代器
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator()); // 添加当前 MemTable 的迭代器
  mem_->Ref(); // 增加 MemTable 引用计数
  if (imm_ != nullptr) { // 如果存在不可变 MemTable
    list.push_back(imm_->NewIterator()); // 添加不可变 MemTable 的迭代器
    imm_->Ref(); // 增加不可变 MemTable 引用计数
  }
  versions_->current()->AddIterators(options, &list); // 添加当前 Version 中所有 SSTable 的迭代器
  // 创建一个合并迭代器，将所有子迭代器合并
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref(); // 增加当前 Version 的引用计数

  // 创建 IterState 对象，用于在迭代器销毁时清理资源
  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  // 注册清理函数
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_; // 生成并返回一个唯一的迭代器种子/ID
  mutex_.Unlock(); // 释放锁
  return internal_iter;
}

// 测试接口：创建一个内部迭代器
Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

// 测试接口：获取下一层级最大重叠字节数
int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

// 获取指定键的值
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_); // 获取锁
  SequenceNumber snapshot; // 用于读取的快照序列号
  if (options.snapshot != nullptr) { // 如果用户指定了快照
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else { // 否则使用最新的序列号作为隐式快照
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_; // 获取当前 MemTable
  MemTable* imm = imm_; // 获取不可变 MemTable (可能为 nullptr)
  Version* current = versions_->current(); // 获取当前 Version
  mem->Ref(); // 增加引用计数
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false; // 标记是否需要更新 Version 的统计信息 (用于触发 Compaction)
  Version::GetStats stats; // 用于收集 Get 操作的统计信息

  // 在从文件和 MemTable 读取数据时释放锁
  // Get 操作期间 Put 不会发生，因为 LevelDB 的公开 API 是单线程的或需要外部同步
  {
    mutex_.Unlock(); // 释放锁
    // 首先在当前 MemTable 中查找
    LookupKey lkey(key, snapshot); // 构建查找键 (包含序列号)
    if (mem->Get(lkey, value, &s)) {
      // 如果在 MemTable 中找到，则完成
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // 如果在不可变 MemTable 中找到，则完成
    } else {  // 在当前 Version 下的 SSTable 中检索
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true; // 标记需要更新统计信息
    }
    mutex_.Lock(); // 重新获取锁
  }

  // 如果 Get 操作涉及到了 SSTable 并且需要更新统计信息，则更新 Version 的统计数据
  // 如果统计数据显示需要 Compaction (例如某个文件被频繁访问)，则尝试调度 Compaction
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref(); // 解引用
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

// 创建一个用户可见的迭代器
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  // 创建一个内部合并迭代器
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  // 包装成 DBIterator，处理用户键、序列号过滤等
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number() // 如果指定了快照，则使用快照的序列号
                            : latest_snapshot), // 否则使用最新序列号
                       seed);
}

// 记录一次读操作的样本 (用于基于访问频率触发 Compaction)
void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) { // 如果记录样本后发现需要 Compaction
    MaybeScheduleCompaction(); // 尝试调度 Compaction
  }
}

// 获取一个数据库快照
const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  // 使用当前最新的序列号创建一个新的快照对象，并加入到快照列表中
  return snapshots_.New(versions_->LastSequence());
}

// 释放一个数据库快照
void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  // 从快照列表中删除指定的快照对象
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Put 操作的便捷方法实现 (调用 DB 基类的 Put)
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

// Delete 操作的便捷方法实现 (调用 DB 基类的 Delete)
Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

// 执行写操作 (Put 或 Delete)
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_); // 创建一个 Writer 对象，代表当前的写请求
  w.batch = updates; // 设置要写入的数据批次
  w.sync = options.sync; // 设置是否需要同步
  w.done = false; // 标记为未完成

  // 1. 队长统一提交，其他调用者等待：
  //    所有写请求都会加入 writers_ 队列。只有队首的 Writer (队长) 才能实际执行写操作。
  //    其他 Writer 会在条件变量上等待，直到轮到它们或者它们的操作被队长合并。
  // 2. 队长写数据期间放开了锁，其他调用者可以继续排队：
  //    在实际写入日志和 MemTable 时，队长会释放锁，允许其他线程加入队列或执行读操作。
  // 3. 队长写数据结束后，一个是通知排队者，一个是通知下一个队长继续：
  //    写操作完成后，队长会通知所有被其合并的 Writer 操作已完成，并唤醒队列中的下一个 Writer。
  MutexLock l(&mutex_); // 获取锁
  writers_.push_back(&w); // 将当前 Writer加入等待队列的末尾
  // 当且仅当当前 Writer 不是队首，并且其操作未完成时，等待
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait(); // 在条件变量上等待
  }
  if (w.done) { // 如果操作已被其他 Writer (队长) 完成
    return w.status; // 直接返回状态
  }
  
  // 当前 Writer 成为队长，负责执行写操作
  // 为写操作准备空间 (可能会暂时解锁并等待 Compaction)
  // 如果 updates 为 nullptr，表示这是一个强制刷新/Compaction 的信号，而不是实际的数据写入
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence(); // 获取当前最新的序列号
  printf("last_sequence=%llu\n",last_sequence); // 调试输出
  Writer* last_writer = &w; // 记录本次批处理的最后一个 Writer (初始为当前 Writer)
  if (status.ok() && updates != nullptr) {  // 如果状态正常且有实际数据要写入 (updates 不为 nullptr 表示不是 Compaction 信号)
    // 构建一个包含多个 Writer 请求的批处理组 (如果可能)
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    // 为整个批处理组设置起始序列号
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    // 更新最新的序列号
    last_sequence += WriteBatchInternal::Count(write_batch);

    // 添加到日志并应用到 MemTable。在此阶段可以释放锁，
    // 因为当前 Writer (&w) 负责日志记录，并防止并发的日志记录器和并发写入 MemTable。
    {
      mutex_.Unlock();  // 队长准备好了 MemTable 空间，后来者均会排队，此处释放锁并进行 I/O
      // 将批处理内容写入日志文件
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false; // 标记同步是否出错
      if (status.ok() && options.sync) { // 如果写入日志成功且需要同步
        status = logfile_->Sync(); // 同步日志文件到磁盘
        if (!status.ok()) {
          sync_error = true; // 记录同步错误
        }
      }
      if (status.ok()) { // 如果日志操作成功 (包括可选的同步)
        // 将批处理内容插入到 MemTable
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock(); // 重新获取锁
      if (sync_error) { // 如果同步出错
        // 日志文件的状态不确定：我们刚刚添加的日志记录可能在数据库重新打开时出现，也可能不出现。
        // 因此，我们强制数据库进入一种模式，所有未来的写入都会失败。
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear(); // 如果使用了临时批处理对象，则清空它

    versions_->SetLastSequence(last_sequence); // 更新 VersionSet 中的最新序列号
  }

  // 通知所有被包含在此次批处理中的 Writer，它们的操作已完成
  while (true) {
    Writer* ready = writers_.front(); // 获取队首的 Writer
    writers_.pop_front(); // 从队列中移除
    if (ready != &w) { // 如果不是当前执行的队长 Writer (即被合并的 Writer)
      ready->status = status; // 设置其状态
      ready->done = true; // 标记为已完成
      ready->cv.Signal(); // 通知其等待的条件变量
    }
    if (ready == last_writer) break; // 如果已处理到本次批处理的最后一个 Writer，则结束
  }

  // 通知写入队列中的新的队首 Writer (如果队列不为空)
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: writers_ 队列不能为空
// REQUIRES: 第一个 Writer 必须有一个非空的 batch
// 将队列中连续的多个写请求合并成一个大的 WriteBatch，以提高吞吐量。
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  assert(!writers_.empty()); // 确保写入队列不为空
  Writer* first = writers_.front(); // 获取队首的 Writer
  WriteBatch* result = first->batch; // 初始结果为第一个 Writer 的 batch
  assert(result != nullptr); // 确保第一个 Writer 的 batch 不为空

  size_t size = WriteBatchInternal::ByteSize(first->batch); // 计算第一个 batch 的大小

  // 允许批处理组增长到最大大小，但如果原始写入很小，
  // 则限制增长，以免过多地减慢小型写入的速度。
  size_t max_size = 1 << 20; // 默认最大批处理大小为 1MB
  if (size <= (128 << 10)) { // 如果原始写入小于等于 128KB
    max_size = size + (128 << 10); // 则最大批处理大小限制为 原始大小 + 128KB
  }

  *last_writer = first; // 初始化最后一个 Writer 为第一个 Writer
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // 从第二个 Writer 开始遍历
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // 不要将需要同步的写入合并到由非同步写入处理的批处理中。
      // 如果队长的 first->sync 是 false，但队列中某个 w->sync 是 true，则不能合并。
      break;
    }

    if (w->batch != nullptr) { // 如果当前 Writer 有数据批次
      size += WriteBatchInternal::ByteSize(w->batch); // 累加大小
      if (size > max_size) {
        // 如果总大小超过最大限制，则不再合并
        break;
      }

      // 追加到 *result
      if (result == first->batch) { // 如果 result 仍然是第一个 Writer 的原始 batch
        // 切换到临时批处理 tmp_batch_，而不是修改调用者的 batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0); // 确保 tmp_batch_ 是空的
        WriteBatchInternal::Append(result, first->batch); // 将第一个 batch 的内容追加到 tmp_batch_
      }
      WriteBatchInternal::Append(result, w->batch); // 将当前 Writer 的 batch 内容追加到 result (可能是 tmp_batch_)
    }
    *last_writer = w; // 更新最后一个 Writer 为当前 Writer
  }
  return result; // 返回合并后的 WriteBatch (可能是第一个 Writer 的原始 batch，也可能是 tmp_batch_)
}

// REQUIRES: mutex_ 已持有
// REQUIRES: 当前线程位于写入队列的前端
// 为写入操作腾出空间。如果 MemTable 已满，则可能需要切换 MemTable 并触发 Compaction。
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld(); // 调用此函数前必须持有互斥锁
  assert(!writers_.empty()); // 确保写入队列不为空
  bool allow_delay = !force; // 如果不是强制操作 (force=false)，则允许延迟
  Status s;
  while (true) { // 循环直到有足够空间或发生错误
    if (!bg_error_.ok()) {
      // 如果发生后台错误，则返回该错误
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // 如果 Level-0 文件数量接近硬限制 (kL0_SlowdownWritesTrigger)，
      // 为了避免在达到硬限制时单个写入延迟数秒，开始对每个单独的写入延迟 1 毫秒，
      // 以减少延迟方差。此外，这种延迟会将一些 CPU 交给 Compaction 线程（如果它与写入线程共享同一核心）。
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000); // 延迟 1 毫秒
      allow_delay = false;  // 单个写入操作只延迟一次
      mutex_.Lock();
    } else if (!force && // 如果不是强制操作
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // 并且当前 MemTable 中有足够空间
      break; // 则退出循环，空间已足够
    } else if (imm_ != nullptr) { // 如果存在不可变的 MemTable (imm_)
      // 当前 MemTable 已满，但上一个 MemTable (imm_) 仍在 Compaction 中，所以等待。
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait(); // 等待后台 Compaction 完成信号
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // 如果 Level-0 文件数量达到硬限制 (kL0_StopWritesTrigger)，则等待。
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait(); // 等待后台 Compaction 完成信号
    } else {
      // 尝试切换到新的 MemTable 并触发旧 MemTable 的 Compaction
      assert(versions_->PrevLogNumber() == 0); // 确保前一个日志编号为0 (已被处理)
      uint64_t new_log_number = versions_->NewFileNumber(); // 获取新的日志文件编号
      WritableFile* lfile = nullptr;
      // 创建新的日志文件
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // 避免在紧密循环中耗尽文件编号空间。
        versions_->ReuseFileNumber(new_log_number); // 如果创建失败，重用该文件编号
        break;
      }

      delete log_; // 删除旧的日志写入器

      // 关闭旧的日志文件
      // 注意：这里没有检查 logfile_ 是否为 nullptr，如果 DB 刚打开且 reuse_logs=false，
      // 第一次 MakeRoomForWrite 时 logfile_ 可能就是 nullptr。
      // 但通常在 DB::Open 成功后，logfile_ 和 log_ 会被正确初始化。
      // 如果 logfile_ 是 nullptr，Close() 应该能安全处理或返回错误。
      if (logfile_ != nullptr) {
          s = logfile_->Close();
          if (!s.ok()) {
            // 我们可能丢失了写入到上一个日志文件的一些数据。
            // 无论如何都切换到新的日志文件，但将其记录为后台错误，
            // 以便我们不再尝试任何写入。
            //
            // 我们或许可以尝试保存与日志文件对应的 MemTable，
            // 如果成功则抑制错误，但这会在关键代码路径中增加更多复杂性。
            RecordBackgroundError(s);
          }
          delete logfile_;
      }


      logfile_ = lfile; // 更新为新的日志文件对象
      logfile_number_ = new_log_number; // 更新当前日志文件编号
      log_ = new log::Writer(lfile); // 创建新的日志写入器
      imm_ = mem_; // 将当前的 MemTable 设为不可变的 MemTable (imm_)
      has_imm_.store(true, std::memory_order_release); // 标记存在 imm_
      mem_ = new MemTable(internal_comparator_); // 创建新的当前 MemTable
      mem_->Ref(); // 增加引用计数
      force = false;  // 如果有空间，则不再强制进行另一次 Compaction
      MaybeScheduleCompaction(); // 尝试调度 Compaction (因为 imm_ 已产生)
    }
  }
  return s;
}

// 获取数据库的属性信息
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear(); // 清空输出字符串

  MutexLock l(&mutex_); // 获取锁
  Slice in = property;
  Slice prefix("leveldb."); // 属性名必须以 "leveldb." 开头
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size()); // 移除前缀

  if (in.starts_with("num-files-at-level")) { // 获取指定层级的文件数量
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty(); // 解析层级编号
    if (!ok || level >= config::kNumLevels) { // 如果解析失败或层级无效
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level))); // 获取文件数量
      *value = buf;
      return true;
    }
  } else if (in == "stats") { // 获取 Compaction 统计信息
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) { // 如果该层级有文件或发生过 Compaction
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0, // 文件总大小 (MB)
                      stats_[level].micros / 1e6, // Compaction 时间 (秒)
                      stats_[level].bytes_read / 1048576.0, // Compaction 读取字节数 (MB)
                      stats_[level].bytes_written / 1048576.0); // Compaction 写入字节数 (MB)
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") { // 获取所有 SSTable 的调试字符串信息
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") { // 获取近似的内存使用量
    size_t total_usage = options_.block_cache->TotalCharge(); // 块缓存使用量
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage(); // 当前 MemTable 使用量
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage(); // 不可变 MemTable 使用量
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false; // 未识别的属性
}

// 获取指定键范围的大致大小
void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): 更好的实现方式
  MutexLock l(&mutex_); // 获取锁
  Version* v = versions_->current(); // 获取当前 Version
  v->Ref(); // 增加引用计数

  for (int i = 0; i < n; i++) {
    // 将用户键转换为对应的内部键
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    // 获取键范围在 SSTable 文件中的大致偏移量
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0); // 计算大小
  }

  v->Unref(); // 解引用 Version
}

// DB 基类中 Put 便捷方法的默认实现
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value); // 将 Put 操作添加到 WriteBatch
  return Write(opt, &batch); // 调用 DBImpl 的 Write 方法执行
}

// DB 基类中 Delete 便捷方法的默认实现
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key); // 将 Delete 操作添加到 WriteBatch
  return Write(opt, &batch); // 调用 DBImpl 的 Write 方法执行
}

DB::~DB() = default; // DB 基类的析构函数 (默认实现)

// 打开数据库的静态工厂方法
Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr; // 初始化输出参数

  DBImpl* impl = new DBImpl(options, dbname); // 创建 DBImpl 实例
  impl->mutex_.Lock(); // 获取锁
  VersionEdit edit; // 用于记录恢复过程中的版本变更
  // Recover 函数处理 create_if_missing 和 error_if_exists 选项
  bool save_manifest = false; // 标记是否需要在恢复后保存 MANIFEST
  // 执行数据库恢复过程：
  // 1. 基于 MANIFEST 恢复 VersionSet。
  // 2. 重做日志文件，更新序列号和文件编号，并将变更记录到 edit。
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) { // 如果恢复成功且当前 MemTable 为空 (例如，新创建的数据库或所有日志都已处理)
    // 创建新的日志文件和对应的 MemTable。
    uint64_t new_log_number = impl->versions_->NewFileNumber(); // 获取新的日志文件编号
    WritableFile* lfile;
    // 创建新的日志文件
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) { // 如果日志文件创建成功
      edit.SetLogNumber(new_log_number); // 在 VersionEdit 中记录新的日志文件编号
      impl->logfile_ = lfile; // 设置当前日志文件对象
      impl->logfile_number_ = new_log_number; // 设置当前日志文件编号
      impl->log_ = new log::Writer(lfile); // 创建新的日志写入器
      impl->mem_ = new MemTable(impl->internal_comparator_); // 创建新的当前 MemTable
      impl->mem_->Ref(); // 增加引用计数
    }
  }
  if (s.ok() && save_manifest) { // 如果恢复成功且需要保存 MANIFEST (例如，日志恢复产生了新的 SSTable)
    edit.SetPrevLogNumber(0);  // 恢复后不再需要更早的日志文件
    edit.SetLogNumber(impl->logfile_number_); // 设置当前日志文件编号
    // 将 VersionEdit 应用到 VersionSet 并记录到 MANIFEST
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  // 恢复 MemTable 并 Compact SSTable 后，再尝试进行一次 SSTable 之间的 Compaction
  if (s.ok()) {
    impl->RemoveObsoleteFiles(); // 移除过时的文件
    impl->MaybeScheduleCompaction(); // 尝试调度 Compaction
  }
  impl->mutex_.Unlock(); // 释放锁
  if (s.ok()) { // 如果整个打开过程成功
    assert(impl->mem_ != nullptr); // 确保当前 MemTable 不为空
    *dbptr = impl; // 设置输出参数为 DBImpl 实例
  } else { // 如果打开失败
    delete impl; // 删除 DBImpl 实例
  }
  return s;
}

Snapshot::~Snapshot() = default; // Snapshot 基类的析构函数 (默认实现)

// 销毁指定的数据库 (删除所有相关文件)
Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env; // 获取环境对象
  std::vector<std::string> filenames; // 用于存储数据库目录下的文件名
  Status result = env->GetChildren(dbname, &filenames); // 获取文件名列表
  if (!result.ok()) {
    // 如果目录不存在，则忽略错误 (认为数据库已销毁)
    return Status::OK();
  }

  FileLock* lock; // 文件锁对象
  const std::string lockname = LockFileName(dbname); // 锁文件名
  result = env->LockFile(lockname, &lock); // 尝试获取数据库锁
  if (result.ok()) { // 如果成功获取锁
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      // 解析文件名，并删除除锁文件外的所有 LevelDB 相关文件
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // 锁文件将在最后删除
        Status del = env->RemoveFile(dbname + "/" + filenames[i]); // 删除文件
        if (result.ok() && !del.ok()) { // 如果之前的 result 正常但删除失败，则更新 result
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // 释放文件锁 (忽略错误，因为状态已经消失)
    env->RemoveFile(lockname); // 删除锁文件
    env->RemoveDir(dbname);  // 删除数据库目录 (如果目录中包含其他文件，则忽略错误)
  }
  return result;
}

}  // namespace leveldb
