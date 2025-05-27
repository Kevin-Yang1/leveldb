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

// 非表缓存文件的数量，例如MANIFEST, CURRENT, LOG等文件
const int kNumNonTableCacheFiles = 10;

// 为每个等待的写入者保留的信息
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {} // 构造函数

  Status status; // 写入操作的状态
  WriteBatch* batch; // 写入批处理
  bool sync; // 是否需要同步写入
  bool done; // 写入是否完成
  port::CondVar cv; // 用于等待写入完成的条件变量
};

// Compaction（合并）过程中的状态信息
struct DBImpl::CompactionState {
  // Compaction产生的文件
  struct Output {
    uint64_t number; // 文件号
    uint64_t file_size; // 文件大小
    InternalKey smallest, largest; // 文件中的最小键和最大键
  };

  // 返回当前正在生成的输出文件信息
  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c) // 构造函数
      : compaction(c),
        smallest_snapshot(0), // 最小快照序列号
        outfile(nullptr), // 输出文件
        builder(nullptr), // 表构造器
        total_bytes(0) {} // 总字节数

  Compaction* const compaction; // 指向Compaction对象的指针

  // 小于 smallest_snapshot 的序列号不重要，
  // 因为我们永远不需要服务于 smallest_snapshot 以下的快照。
  // 因此，如果我们看到了一个序列号 S <= smallest_snapshot，
  // 我们可以丢弃所有具有相同键且序列号 < S 的条目。
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs; // Compaction输出文件列表

  // 为正在生成的输出文件保留的状态
  WritableFile* outfile; // 可写文件对象
  TableBuilder* builder; // 表构造器对象

  uint64_t total_bytes; // Compaction过程中写入的总字节数
};

// 将用户提供选项修正到合理范围
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

// 规范化用户提供的Options
Options SanitizeOptions(const std::string& dbname, // 数据库名称
                        const InternalKeyComparator* icmp, // 内部键比较器
                        const InternalFilterPolicy* ipolicy, // 内部过滤器策略
                        const Options& src) { // 源Options对象
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  // 调整最大打开文件数范围
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  // 调整写缓冲区大小范围
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  // 调整最大文件大小范围
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  // 调整块大小范围
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // 在与数据库相同的目录中打开一个日志文件
    src.env->CreateDir(dbname);  // 如果目录不存在则创建
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname)); // 重命名旧的日志文件
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log); // 创建新的日志记录器
    if (!s.ok()) {
      // 没有合适的地方记录日志
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    // 如果块缓存为空，则创建一个默认大小的LRU缓存 (8MB)
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

// 计算表缓存的大小
static int TableCacheSize(const Options& sanitized_options) {
  // 为其他用途保留大约10个文件，其余的分配给TableCache。
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// DBImpl的构造函数
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env), // 环境对象
      internal_comparator_(raw_options.comparator), // 内部键比较器
      internal_filter_policy_(raw_options.filter_policy), // 内部过滤器策略
      options_(SanitizeOptions(dbname, &internal_comparator_, // 规范化后的选项
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log), // 是否拥有info_log的所有权
      owns_cache_(options_.block_cache != raw_options.block_cache), // 是否拥有cache的所有权
      dbname_(dbname), // 数据库名称
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))), // 表缓存
      db_lock_(nullptr), // 数据库锁文件
      shutting_down_(false), // 是否正在关闭
      background_work_finished_signal_(&mutex_), // 后台工作完成信号
      mem_(nullptr), // 当前的memtable
      imm_(nullptr), // 不可变的memtable，等待被compact
      has_imm_(false), // 是否存在不可变的memtable
      logfile_(nullptr), // 当前日志文件
      logfile_number_(0), // 当前日志文件编号
      log_(nullptr), // 日志写入器
      seed_(0), // 用于生成迭代器ID的种子
      tmp_batch_(new WriteBatch), // 用于合并写入的临时批处理
      background_compaction_scheduled_(false), // 是否已调度后台compaction
      manual_compaction_(nullptr), // 手动compaction请求
      versions_(new VersionSet(dbname_, &options_, table_cache_, // 版本集
                               &internal_comparator_)) {}

// DBImpl的析构函数
DBImpl::~DBImpl() {
  // 等待后台工作完成
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release); // 设置正在关闭标志
  while (background_compaction_scheduled_) { // 等待后台compaction完成
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_); // 解锁数据库锁文件
  }

  delete versions_; // 删除版本集
  if (mem_ != nullptr) mem_->Unref(); // 减少memtable引用计数
  if (imm_ != nullptr) imm_->Unref(); // 减少不可变memtable引用计数
  delete tmp_batch_; // 删除临时批处理
  delete log_; // 删除日志写入器
  delete logfile_; // 删除日志文件
  delete table_cache_; // 删除表缓存

  if (owns_info_log_) { // 如果拥有info_log的所有权，则删除它
    delete options_.info_log;
  }
  if (owns_cache_) { // 如果拥有cache的所有权，则删除它
    delete options_.block_cache;
  }
}

// 创建一个新的数据库
Status DBImpl::NewDB() {
  VersionEdit new_db; // 创建一个新的版本编辑
  new_db.SetComparatorName(user_comparator()->Name()); // 设置比较器名称
  new_db.SetLogNumber(0); // 设置日志文件编号
  new_db.SetNextFile(2); // 设置下一个文件编号 (1是MANIFEST)
  new_db.SetLastSequence(0); // 设置最后的序列号

  const std::string manifest = DescriptorFileName(dbname_, 1); // MANIFEST文件名
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file); // 创建MANIFEST文件
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file); // 创建日志写入器写入MANIFEST
    std::string record;
    new_db.EncodeTo(&record); // 将VersionEdit编码到字符串
    s = log.AddRecord(record); // 添加记录到MANIFEST
    if (s.ok()) {
      s = file->Sync(); // 同步文件内容到磁盘
    }
    if (s.ok()) {
      s = file->Close(); // 关闭文件
    }
  }
  delete file;
  if (s.ok()) {
    // 创建 "CURRENT" 文件，指向新的 MANIFEST 文件。
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest); // 如果出错，删除MANIFEST文件
  }
  return s;
}

// 根据 paranoid_checks 选项决定是否忽略错误
void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // 不需要改变
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK(); // 将错误状态置为OK
  }
}

// 删除过时的文件
void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld(); // 确保持有锁

  if (!bg_error_.ok()) {
    // 后台发生错误后，我们不知道新版本是否已提交，
    // 因此不能安全地进行垃圾回收。
    return;
  }

  // 创建一个包含所有活动文件的集合
  std::set<uint64_t> live = pending_outputs_; // 正在生成的输出文件是活动的
  versions_->AddLiveFiles(&live); // 将版本集中的活动文件添加到集合中

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // 获取数据库目录下的所有文件名 (故意忽略错误)
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete; // 待删除的文件列表
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) { // 解析文件名
      bool keep = true; // 默认保留文件
      switch (type) {
        case kLogFile: // 日志文件
          keep = ((number >= versions_->LogNumber()) || // 保留当前或更新的日志文件
                  (number == versions_->PrevLogNumber())); // 保留前一个日志文件 (兼容旧版本)
          break;
        case kDescriptorFile: // MANIFEST 文件
          // 保留我的 MANIFEST 文件，以及任何更新的 MANIFEST 文件
          // (以防存在允许其他实例的竞争条件)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile: // SSTable 文件
          keep = (live.find(number) != live.end()); // 保留活动SSTable文件
          break;
        case kTempFile: // 临时文件
          // 当前正在写入的任何临时文件都必须记录在 pending_outputs_ 中，
          // pending_outputs_ 已插入到 "live" 集合中
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile: // CURRENT 文件
        case kDBLockFile: // 数据库锁文件
        case kInfoLogFile: // 信息日志文件
          keep = true; // 这些文件总是保留
          break;
      }

      if (!keep) { // 如果不保留
        files_to_delete.push_back(std::move(filename)); // 添加到待删除列表
        if (type == kTableFile) {
          table_cache_->Evict(number); // 从表缓存中移除
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // 删除所有文件时，解锁以允许其他线程继续。所有被删除的文件
  // 都有唯一的名称，不会与新创建的文件冲突，
  // 因此在允许其他线程继续进行的同时删除是安全的。
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename); // 删除文件
  }
  mutex_.Lock(); // 重新获取锁
}

// 从磁盘恢复数据库状态
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld(); // 确保持有锁

  // 忽略 CreateDir 的错误，因为数据库的创建仅在描述符创建时才提交，
  // 并且此目录可能已因先前失败的创建尝试而存在。
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_); // 锁定数据库
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) { // 如果CURRENT文件不存在
    if (options_.create_if_missing) { // 如果选项允许创建
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB(); // 创建新数据库
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else { // 如果CURRENT文件存在
    if (options_.error_if_exists) { // 如果选项要求数据库不存在
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest); // 从版本集恢复
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0); // 记录恢复过程中遇到的最大序列号

  // 从比描述符中指定的日志文件更新的日志文件中恢复
  // (新的日志文件可能是由前一个实例添加的，但没有在描述符中注册)。
  //
  // 注意 PrevLogNumber() 不再使用，但我们关注它，以防我们
  // 正在恢复由旧版本 leveldb 生成的数据库。
  const uint64_t min_log = versions_->LogNumber(); // MANIFEST中记录的最小日志号
  const uint64_t prev_log = versions_->PrevLogNumber(); // MANIFEST中记录的前一个日志号 (兼容旧版)
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames); // 获取数据库目录下的所有文件
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected; // 期望存在的SSTable文件
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs; // 需要恢复的日志文件列表
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number); // 如果文件存在，从期望列表中移除
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number); // 收集需要恢复的日志文件
    }
  }
  if (!expected.empty()) { // 如果有期望的SSTable文件丢失
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // 按日志生成的顺序恢复
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence); // 恢复单个日志文件
    if (!s.ok()) {
      return s;
    }

    // 前一个实例可能在分配此日志号后没有写入任何 MANIFEST 记录。
    // 因此，我们手动更新 VersionSet 中的文件号分配计数器。
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) { // 更新数据库的最后序列号
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

// 从指定的日志文件恢复数据
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log, // log文件号, 是否是最后一个log文件
                              bool* save_manifest, VersionEdit* edit, // 是否需要保存manifest, 版本编辑
                              SequenceNumber* max_sequence) { // 最大序列号
  // 日志读取报告器，用于处理损坏的日志记录
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // 如果 options_.paranoid_checks==false 则为 null
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s; // 记录第一个遇到的损坏错误
    }
  };

  mutex_.AssertHeld(); // 确保持有锁

  // 打开日志文件
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file); // 创建顺序读文件对象
  if (!status.ok()) {
    MaybeIgnoreError(&status); // 根据选项决定是否忽略错误
    return status;
  }

  // 创建日志读取器。
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr); // 如果开启了偏执检查，则传递status指针
  // 我们故意让 log::Reader 进行校验和检查，即使 paranoid_checks==false，
  // 以便损坏导致整个提交被跳过，而不是传播错误信息（例如过大的序列号）。
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // 读取所有记录并添加到 memtable
  std::string scratch; // 临时空间
  Slice record; // 读取到的记录
  WriteBatch batch; // 写入批处理
  int compactions = 0; // 在恢复此日志期间执行的compaction次数
  MemTable* mem = nullptr; // 用于恢复的memtable
  while (reader.ReadRecord(&record, &scratch) && status.ok()) { // 循环读取记录
    if (record.size() < 12) { // 记录头部至少12字节 (seq 8B + count 4B)
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record); // 将记录内容设置到WriteBatch

    if (mem == nullptr) { // 如果memtable为空，则创建一个新的
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem); // 将WriteBatch插入到memtable
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1; // 计算此批处理的最大序列号
    if (last_seq > *max_sequence) { // 更新全局最大序列号
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) { // 如果memtable大小超过阈值
      compactions++;
      *save_manifest = true; // 标记需要保存manifest
      status = WriteLevel0Table(mem, edit, nullptr); // 将memtable刷到Level-0 SSTable
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // 立即反映错误，以便像文件系统已满这样的情况导致 DB::Open() 失败。
        break;
      }
    }
  }

  delete file; // 删除文件对象

  // 查看是否应该继续重用最后一个日志文件。
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() && // 获取文件大小
        env_->NewAppendableFile(fname, &logfile_).ok()) { // 以追加模式打开文件
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size); // 创建新的日志写入器，从文件末尾开始
      logfile_number_ = log_number;
      if (mem != nullptr) { // 如果恢复过程中有未刷盘的memtable
        mem_ = mem; // 将其作为当前的memtable
        mem = nullptr;
      } else {
        // 如果lognum存在但是空的，mem可能为nullptr。
        mem_ = new MemTable(internal_comparator_); // 创建一个新的空memtable
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) { // 如果还有未处理的memtable (没有被重用)
    // mem 没有被重用；将其 compact。
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr); // 将其刷到Level-0
    }
    mem->Unref();
  }

  return status;
}

// 将 MemTable 的内容写入 Level-0 的 SSTable 文件
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) { // memtable, 版本编辑, 基础版本
  mutex_.AssertHeld(); // 确保持有锁
  const uint64_t start_micros = env_->NowMicros(); // 记录开始时间
  FileMetaData meta; // 文件元数据
  meta.number = versions_->NewFileNumber(); // 获取新的文件编号
  pending_outputs_.insert(meta.number); // 将文件编号加入到正在生成的输出集合中
  Iterator* iter = mem->NewIterator(); // 创建memtable的迭代器
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock(); // 解锁，因为BuildTable耗时较长
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta); // 构建SSTable
    mutex_.Lock(); // 重新获取锁
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter; // 删除迭代器
  pending_outputs_.erase(meta.number); // 从正在生成的输出集合中移除

  // 注意，如果 file_size 为零，则文件已被删除，不应添加到 manifest 中。
  int level = 0; // 目标层级，对于memtable dump总是0，除非有base version用于选择更优层级
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) { // 如果提供了基础版本，尝试选择一个更优的层级（通常用于immutable memtable compaction）
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest, // 将新文件添加到版本编辑中
                  meta.largest);
  }

  CompactionStats stats; // Compaction统计信息
  stats.micros = env_->NowMicros() - start_micros; // 计算耗时
  stats.bytes_written = meta.file_size; // 写入的字节数
  stats_[level].Add(stats); // 更新该层级的统计信息
  return s;
}

// Compact 不可变的 MemTable (imm_)
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld(); // 确保持有锁
  assert(imm_ != nullptr); // 不可变memtable必须存在

  // 将 memtable 的内容保存为一个新的 Table
  VersionEdit edit;
  Version* base = versions_->current(); // 获取当前版本作为基础
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base); // 将imm_写入SSTable
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) { // 如果在关闭过程中
    s = Status::IOError("Deleting DB during memtable compaction"); // 返回IO错误
  }

  // 用生成的 Table 替换不可变的 memtable
  if (s.ok()) {
    edit.SetPrevLogNumber(0); // 之前的日志不再需要
    edit.SetLogNumber(logfile_number_);  // 设置当前日志文件编号
    s = versions_->LogAndApply(&edit, &mutex_); // 应用版本编辑并记录到MANIFEST
  }

  if (s.ok()) {
    // 提交到新状态
    imm_->Unref(); // 减少imm_的引用计数
    imm_ = nullptr; // 置空imm_
    has_imm_.store(false, std::memory_order_release); // 更新has_imm_标志
    RemoveObsoleteFiles(); // 删除过时的文件
  } else {
    RecordBackgroundError(s); // 记录后台错误
  }
}

// Compact 指定范围的键
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1; // 初始化包含文件的最大层级
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) { // 遍历所有层级
      if (base->OverlapInLevel(level, begin, end)) { // 如果该层级与指定范围有重叠
        max_level_with_files = level; // 更新最大层级
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): 如果memtable没有重叠，则跳过
  for (int level = 0; level < max_level_with_files; level++) { // 从level-0开始逐层compact
    TEST_CompactRange(level, begin, end); // 对指定层级和范围进行compaction (测试接口)
  }
}

// 测试接口：Compact 指定层级和范围
void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels); // 确保下一层级有效

  InternalKey begin_storage, end_storage; // 用于存储转换后的内部键

  ManualCompaction manual; // 手动compaction请求
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) { // 如果起始键为空
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek); // 构造起始内部键
    manual.begin = &begin_storage;
  }
  if (end == nullptr) { // 如果结束键为空
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0)); // 构造结束内部键
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) && // 循环直到手动compaction完成、数据库关闭或发生后台错误
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // 如果当前没有手动compaction在运行 (空闲状态)
      manual_compaction_ = &manual; // 设置当前手动compaction请求
      MaybeScheduleCompaction(); // 尝试调度compaction
    } else {  // 正在运行我自己的compaction或其他compaction。
      background_work_finished_signal_.Wait(); // 等待后台工作完成
    }
  }
  // 在 `background_work_finished_signal_` 因错误而被发出信号的情况下，完成当前的后台compaction。
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  if (manual_compaction_ == &manual) {
    // 由于某种原因提前中止，取消我的手动compaction。
    manual_compaction_ = nullptr;
  }
}

// 测试接口：Compact MemTable
Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch 表示仅等待较早的写入完成
  Status s = Write(WriteOptions(), nullptr); // 发起一个空写操作，确保之前的写操作完成并可能触发memtable切换
  if (s.ok()) {
    // 等待 compaction 完成
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) { // 等待不可变memtable被compact掉
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) { // 如果等待后imm_仍然存在，说明发生了错误
      s = bg_error_;
    }
  }
  return s;
}

// 记录后台发生的错误
void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld(); // 确保持有锁
  if (bg_error_.ok()) { // 只记录第一个错误
    bg_error_ = s;
    background_work_finished_signal_.SignalAll(); // 通知所有等待后台工作的线程
  }
}

// 尝试调度后台 Compaction
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld(); // 确保持有锁
  if (background_compaction_scheduled_) {
    // 已经调度过了
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // 数据库正在删除；不再进行后台 compaction
  } else if (!bg_error_.ok()) {
    // 已经发生错误；不再进行更改
  } else if (imm_ == nullptr && manual_compaction_ == nullptr && // 如果没有不可变memtable，没有手动compaction请求
             !versions_->NeedsCompaction()) { // 并且版本集认为不需要compaction
    // 没有工作要做
  } else {
    background_compaction_scheduled_ = true; // 标记已调度
    env_->Schedule(&DBImpl::BGWork, this); // 将后台工作函数加入到环境的调度队列
  }
}

// 后台工作的静态包装函数，用于传递给 Env::Schedule
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall(); // 调用实际的后台处理函数
}

// 后台调用的主函数
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_); // 获取锁
  assert(background_compaction_scheduled_); // 确认已调度
  if (shutting_down_.load(std::memory_order_acquire)) {
    // 关闭时不再进行后台工作。
  } else if (!bg_error_.ok()) {
    // 后台发生错误后不再进行后台工作。
  } else {
    BackgroundCompaction(); // 执行后台compaction
  }

  background_compaction_scheduled_ = false; // 清除已调度标志

  // 上一次 compaction 可能在某个层级产生了过多的文件，
  // 因此如果需要，重新调度另一次 compaction。
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll(); // 通知所有等待后台工作的线程
}

// 执行后台 Compaction 的核心逻辑
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld(); // 确保持有锁

  if (imm_ != nullptr) { // 优先处理不可变memtable的compaction
    CompactMemTable();
    return;
  }

  Compaction* c; // Compaction对象
  bool is_manual = (manual_compaction_ != nullptr); // 是否是手动compaction
  InternalKey manual_end; // 手动compaction的结束键 (用于日志)
  if (is_manual) { // 如果是手动compaction
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end); // 从版本集中选择手动compaction任务
    m->done = (c == nullptr); // 如果没有可选的compaction，则标记完成
    if (c != nullptr) { // 如果选择了compaction
      // 获取compaction输入文件的最大键，用于日志记录和下一次手动compaction的起始点
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else { // 自动compaction
    c = versions_->PickCompaction(); // 从版本集中选择一个自动compaction任务
  }

  Status status;
  if (c == nullptr) {
    // 无事可做
  } else if (!is_manual && c->IsTrivialMove()) { // 如果不是手动且是简单的移动操作 (文件直接移动到下一层)
    // 将文件移动到下一层
    assert(c->num_input_files(0) == 1); // 简单移动只涉及一个输入文件
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number); // 从当前层级移除文件
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest, // 添加到下一层级
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_); // 应用版本编辑
    if (!status.ok()) {
      RecordBackgroundError(status); // 记录错误
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else { // 执行实际的合并compaction
    CompactionState* compact = new CompactionState(c); // 创建compaction状态对象
    status = DoCompactionWork(compact); // 执行compaction工作
    if (!status.ok()) {
      RecordBackgroundError(status); // 记录错误
    }
    CleanupCompaction(compact); // 清理compaction状态
    c->ReleaseInputs(); // 释放compaction的输入 (减少引用计数)
    RemoveObsoleteFiles(); // 删除过时的文件
  }
  delete c; // 删除compaction对象

  if (status.ok()) {
    // 完成
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // 忽略关闭期间发现的 compaction 错误
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) { // 如果是手动compaction
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) { // 如果发生错误
      m->done = true; // 标记手动compaction完成 (即使失败)
    }
    if (!m->done) { // 如果手动compaction未完成 (例如只完成了一部分范围)
      // 我们只 compact 了请求范围的一部分。更新 *m
      // 到剩余待 compact 的范围。
      m->tmp_storage = manual_end; // 使用本次compaction的结束键作为下一次的起始键
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr; // 清除当前手动compaction请求
  }
}

// 清理 CompactionState 对象
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld(); // 确保持有锁
  if (compact->builder != nullptr) { // 如果表构造器存在
    // 可能在 compaction 中途收到关闭调用时发生
    compact->builder->Abandon(); // 放弃构造
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr); // 如果构造器为空，输出文件也应为空
  }
  delete compact->outfile; // 删除输出文件对象
  for (size_t i = 0; i < compact->outputs.size(); i++) { // 遍历所有输出文件
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number); // 从正在生成的输出集合中移除
  }
  delete compact; // 删除CompactionState对象
}

// 为 Compaction 打开输出文件
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr); // 此时不应有表构造器
  uint64_t file_number;
  {
    mutex_.Lock(); // 加锁以获取新文件号并更新pending_outputs_
    file_number = versions_->NewFileNumber(); // 获取新的文件编号
    pending_outputs_.insert(file_number); // 加入到正在生成的输出集合
    CompactionState::Output out; // 创建新的输出文件信息
    out.number = file_number;
    out.smallest.Clear(); // 初始化最小键
    out.largest.Clear(); // 初始化最大键
    compact->outputs.push_back(out); // 添加到compaction状态的输出列表
    mutex_.Unlock();
  }

  // 创建输出文件
  std::string fname = TableFileName(dbname_, file_number); // 构造SSTable文件名
  Status s = env_->NewWritableFile(fname, &compact->outfile); // 创建可写文件
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile); // 创建表构造器
  }
  return s;
}

// 完成 Compaction 输出文件的写入
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) { // Compaction状态, 输入迭代器
  assert(compact != nullptr);
  assert(compact->outfile != nullptr); // 输出文件必须存在
  assert(compact->builder != nullptr); // 表构造器必须存在

  const uint64_t output_number = compact->current_output()->number; // 获取当前输出文件的编号
  assert(output_number != 0);

  // 检查迭代器错误
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries(); // 获取当前文件中的条目数
  if (s.ok()) {
    s = compact->builder->Finish(); // 完成表构造
  } else {
    compact->builder->Abandon(); // 如果迭代器出错，则放弃构造
  }
  const uint64_t current_bytes = compact->builder->FileSize(); // 获取文件大小
  compact->current_output()->file_size = current_bytes; // 更新输出文件信息中的大小
  compact->total_bytes += current_bytes; // 更新总写入字节数
  delete compact->builder; // 删除表构造器
  compact->builder = nullptr;

  // 完成并检查文件错误
  if (s.ok()) {
    s = compact->outfile->Sync(); // 同步文件内容到磁盘
  }
  if (s.ok()) {
    s = compact->outfile->Close(); // 关闭文件
  }
  delete compact->outfile; // 删除文件对象
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) { // 如果成功且文件非空
    // 验证表是否可用
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes); // 创建迭代器进行验证
    s = iter->status(); // 检查迭代器状态
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

// 安装 Compaction 结果 (更新 VersionSet)
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld(); // 确保持有锁
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(), // 输入层0的文件数和层级
      compact->compaction->num_input_files(1), compact->compaction->level() + 1, // 输入层1的文件数和层级
      static_cast<long long>(compact->total_bytes)); // 输出的总字节数

  // 添加 compaction 输出
  compact->compaction->AddInputDeletions(compact->compaction->edit()); // 将输入文件标记为删除
  const int level = compact->compaction->level(); // 获取compaction的原始层级
  for (size_t i = 0; i < compact->outputs.size(); i++) { // 遍历所有输出文件
    const CompactionState::Output& out = compact->outputs[i];
    // 将新生成的文件添加到下一层级 (level + 1)
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_); // 应用版本编辑并记录到MANIFEST
}

// 执行 Compaction 的主要工作流程
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros(); // 记录开始时间
  int64_t imm_micros = 0;  // 用于 imm_ compaction 的微秒数

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0); // 确保输入层有文件
  assert(compact->builder == nullptr); // 初始时表构造器应为空
  assert(compact->outfile == nullptr); // 初始时输出文件应为空
  if (snapshots_.empty()) { // 如果没有快照
    compact->smallest_snapshot = versions_->LastSequence(); // 使用最新的序列号
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number(); // 使用最老的快照序列号
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction); // 创建合并迭代器遍历所有输入文件

  // 在实际执行 compaction 工作时释放互斥锁
  mutex_.Unlock();

  input->SeekToFirst(); // 定位到第一个键值对
  Status status;
  ParsedInternalKey ikey; // 解析后的内部键
  std::string current_user_key; // 当前处理的用户键
  bool has_current_user_key = false; // 是否有当前用户键
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber; // 当前用户键的最新序列号
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) { // 迭代器有效且数据库未关闭
    // 优先处理不可变 memtable 的 compaction 工作
    if (has_imm_.load(std::memory_order_relaxed)) { // 如果存在不可变memtable
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock(); // 加锁以检查和处理imm_
      if (imm_ != nullptr) {
        CompactMemTable(); // Compact不可变memtable
        // 如有必要，唤醒 MakeRoomForWrite()。
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start); // 累加imm_ compaction耗时
    }

    Slice key = input->key(); // 获取当前键
    if (compact->compaction->ShouldStopBefore(key) && // 如果当前键超出了本次compaction的输出文件范围
        compact->builder != nullptr) { // 并且当前有正在构建的输出文件
      status = FinishCompactionOutputFile(compact, input); // 完成当前输出文件
      if (!status.ok()) {
        break;
      }
    }

    // 处理键/值，添加到状态等。
    bool drop = false; // 是否丢弃当前键值对
    if (!ParseInternalKey(key, &ikey)) { // 解析内部键
      // 不要隐藏错误键
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key || // 如果是新的用户键
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // 此用户键的首次出现
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber; // 重置该用户键的最新序列号
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // 被同一用户键的更新条目隐藏 (因为我们按序列号降序处理相同user_key的条目)
        drop = true;  // (A) 规则: 如果当前user_key的最新可见版本(last_sequence_for_key)
                      // 已经小于等于最小快照，那么这个条目以及后续所有相同user_key的更旧条目
                      // 对于所有现有和未来的快照都是不可见的，可以丢弃。
      } else if (ikey.type == kTypeDeletion && // 如果是删除标记
                 ikey.sequence <= compact->smallest_snapshot && // 且其序列号小于等于最小快照
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) { // 并且该键在更底层没有数据
        // 对于此用户键：
        // (1) 更高层级没有数据 (compaction的定义)
        // (2) 更低层级的数据将具有更大的序列号 (LevelDB的写入逻辑，实际上是更小，这里可能有误解或特定上下文)
        //    更正: (2) 更低层级的数据如果存在，其序列号必然小于当前删除标记的序列号，
        //              或者说，如果这个删除标记要覆盖更低层的数据，那么它必须是最新的。
        //              这里的逻辑是，如果这个删除标记本身已经对所有快照不可见，
        //              并且它已经是其user_key在compaction输入中的“基础”版本（即没有更旧的版本被它覆盖），
        //              那么这个删除标记就是过时的。
        // (3) 此处正在 compact 的层中具有较小序列号的数据将在接下来的几次循环迭代中被丢弃（通过上面的规则 (A)）。
        // 因此，此删除标记已过时，可以丢弃。
        drop = true;
      }

      last_sequence_for_key = ikey.sequence; // 更新当前用户键的最新处理序列号
    }
#if 0 // 调试日志
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) { // 如果不丢弃
      // 如有必要，打开输出文件
      if (compact->builder == nullptr) { // 如果当前没有输出文件
        status = OpenCompactionOutputFile(compact); // 打开一个新的输出文件
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) { // 如果是文件的第一个条目
        compact->current_output()->smallest.DecodeFrom(key); // 设置文件的最小键
      }
      compact->current_output()->largest.DecodeFrom(key); // 更新文件的最大键
      compact->builder->Add(key, input->value()); // 添加键值对到输出文件

      // 如果输出文件足够大，则关闭它
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) { // 达到最大输出文件大小
        status = FinishCompactionOutputFile(compact, input); // 完成当前输出文件
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next(); // 移动到下一个键值对
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) { // 如果在关闭过程中
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) { // 如果还有未完成的输出文件
    status = FinishCompactionOutputFile(compact, input); // 完成它
  }
  if (status.ok()) {
    status = input->status(); // 获取迭代器的最终状态
  }
  delete input; // 删除输入迭代器
  input = nullptr;

  CompactionStats stats; // Compaction统计
  stats.micros = env_->NowMicros() - start_micros - imm_micros; // 计算总耗时 (排除imm_ compaction时间)
  for (int which = 0; which < 2; which++) { // 遍历输入层 (0和1)
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size; // 累加读取字节数
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size; // 累加写入字节数
  }

  mutex_.Lock(); // 加锁以更新统计信息和版本集
  stats_[compact->compaction->level() + 1].Add(stats); // 更新下一层级的统计信息

  if (status.ok()) {
    status = InstallCompactionResults(compact); // 安装compaction结果
  }
  if (!status.ok()) {
    RecordBackgroundError(status); // 记录错误
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp)); // 记录compaction后的层级摘要
  return status;
}

namespace { // 匿名命名空间

// 迭代器状态，用于管理迭代器生命周期中需要锁保护的资源
struct IterState {
  port::Mutex* const mu; // 互斥锁指针
  Version* const version GUARDED_BY(mu); // 版本对象，受互斥锁保护
  MemTable* const mem GUARDED_BY(mu); // MemTable对象，受互斥锁保护
  MemTable* const imm GUARDED_BY(mu); // 不可变MemTable对象，受互斥锁保护

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

// 迭代器清理函数，在迭代器销毁时调用
static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock(); // 加锁
  state->mem->Unref(); // 减少MemTable引用计数
  if (state->imm != nullptr) state->imm->Unref(); // 减少不可变MemTable引用计数
  state->version->Unref(); // 减少Version引用计数
  state->mu->Unlock(); // 解锁
  delete state; // 删除IterState对象
}

}  // 匿名命名空间结束

// 创建内部迭代器，合并 MemTable, Immutable MemTable 和 SSTable 文件
Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot, // 输出最新的快照序列号
                                      uint32_t* seed) { // 输出迭代器种子
  mutex_.Lock(); // 加锁
  *latest_snapshot = versions_->LastSequence(); // 获取当前最新的序列号作为快照

  // 收集所有需要的子迭代器
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator()); // MemTable迭代器
  mem_->Ref(); // 增加MemTable引用计数
  if (imm_ != nullptr) { // 如果存在不可变MemTable
    list.push_back(imm_->NewIterator()); // 不可变MemTable迭代器
    imm_->Ref(); // 增加不可变MemTable引用计数
  }
  versions_->current()->AddIterators(options, &list); // 添加当前版本SSTable文件的迭代器
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size()); // 创建合并迭代器
  versions_->current()->Ref(); // 增加当前版本引用计数

  // 创建IterState用于管理资源释放
  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr); // 注册清理函数

  *seed = ++seed_; // 生成并返回迭代器种子 (用于区分不同迭代器实例，防止DBIterator的Seek优化问题)
  mutex_.Unlock(); // 解锁
  return internal_iter;
}

// 测试接口：创建内部迭代器
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
  MutexLock l(&mutex_); // 加锁
  SequenceNumber snapshot; // 快照序列号
  if (options.snapshot != nullptr) { // 如果指定了快照
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else { // 否则使用最新序列号
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_; // 当前MemTable
  MemTable* imm = imm_; // 不可变MemTable
  Version* current = versions_->current(); // 当前版本
  mem->Ref(); // 增加引用计数
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false; // 是否需要更新文件读取统计信息
  Version::GetStats stats; // 文件读取统计

  // 解锁，在从文件和 memtable 读取时
  {
    mutex_.Unlock();
    // 首先在 memtable 中查找，然后（如果有的话）在不可变的 memtable 中查找。
    LookupKey lkey(key, snapshot); // 构造查找键
    if (mem->Get(lkey, value, &s)) { // 从MemTable查找
      // 完成
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) { // 从不可变MemTable查找
      // 完成
    } else { // 从SSTable文件查找
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true; // 标记需要更新统计信息
    }
    mutex_.Lock(); // 重新加锁
  }

  if (have_stat_update && current->UpdateStats(stats)) { // 如果需要更新统计信息并且统计信息指示需要compaction
    MaybeScheduleCompaction(); // 尝试调度compaction
  }
  mem->Unref(); // 减少引用计数
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

// 创建一个新的迭代器
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed); // 创建内部合并迭代器
  // 创建DBIterator，包装内部迭代器，处理用户键和快照
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number() // 使用指定快照
                            : latest_snapshot), // 否则使用最新快照
                       seed);
}

// 记录读取样本，用于基于读取频率触发compaction (LevelDB此功能未完全实现或默认关闭)
void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) { // 记录读取样本并检查是否需要compaction
    MaybeScheduleCompaction();
  }
}

// 获取当前数据库状态的快照
const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence()); // 创建并返回一个新的快照对象
}

// 释放快照
void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot)); // 删除快照对象
}

// Put 操作的便捷方法
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val); // 调用基类的Put实现
}

// Delete 操作的便捷方法
Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key); // 调用基类的Delete实现
}

// 执行写入操作 (Put, Delete, WriteBatch)
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_); // 创建一个Writer对象，代表当前的写请求
  w.batch = updates; // 设置写批处理
  w.sync = options.sync; // 设置是否同步
  w.done = false; // 标记为未完成

  MutexLock l(&mutex_); // 加锁
  writers_.push_back(&w); // 将当前Writer加入等待队列的末尾
  while (!w.done && &w != writers_.front()) { // 如果当前Writer未完成且不是队列头部，则等待
    w.cv.Wait(); // 等待被唤醒 (通常是前一个Writer完成后)
  }
  if (w.done) { // 如果已经被其他操作标记为完成 (例如，前一个Writer失败导致整个组失败)
    return w.status; // 返回状态
  }

  // 可能临时解锁并等待。
  Status status = MakeRoomForWrite(updates == nullptr); // 为写入腾出空间 (可能切换memtable)
                                                        // updates == nullptr 表示是compaction触发的写(空写，用于同步)
  uint64_t last_sequence = versions_->LastSequence(); // 获取当前最新序列号
  Writer* last_writer = &w; // 记录批处理组的最后一个Writer
  if (status.ok() && updates != nullptr) {  // nullptr batch 用于 compaction，不需要实际写入
    WriteBatch* write_batch = BuildBatchGroup(&last_writer); // 构建写入批处理组 (合并多个写请求)
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1); // 设置批处理的起始序列号
    last_sequence += WriteBatchInternal::Count(write_batch); // 更新最新序列号

    // 添加到日志并应用到 memtable。在此阶段我们可以释放锁，
    // 因为 &w 当前负责日志记录，并防止并发的日志记录器和并发写入 mem_。
    {
      mutex_.Unlock(); // 解锁，执行IO操作
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch)); // 将批处理内容写入日志
      bool sync_error = false;
      if (status.ok() && options.sync) { // 如果需要同步
        status = logfile_->Sync(); // 同步日志文件到磁盘
        if (!status.ok()) {
          sync_error = true; // 记录同步错误
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_); // 将批处理插入到MemTable
      }
      mutex_.Lock(); // 重新加锁
      if (sync_error) { // 如果同步失败
        // 日志文件的状态不确定：我们刚刚添加的日志记录在数据库重新打开时可能出现也可能不出现。
        // 因此，我们强制数据库进入所有未来写入都失败的模式。
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear(); // 如果使用了临时批处理，则清空它

    versions_->SetLastSequence(last_sequence); // 更新版本集的最新序列号
  }

  // 依次处理队列头部的Writer，直到处理完当前批处理组的所有Writer
  while (true) {
    Writer* ready = writers_.front(); // 获取队列头部的Writer
    writers_.pop_front(); // 从队列中移除
    if (ready != &w) { // 如果不是当前发起操作的Writer (即批处理组中的其他Writer)
      ready->status = status; // 设置其状态
      ready->done = true; // 标记为完成
      ready->cv.Signal(); // 唤醒等待它的线程
    }
    if (ready == last_writer) break; // 如果已处理到批处理组的最后一个Writer，则结束
  }

  // 通知写入队列的新头部
  if (!writers_.empty()) {
    writers_.front()->cv.Signal(); // 唤醒下一个等待的Writer
  }

  return status;
}

// 要求：Writer 列表不能为空
// 要求：第一个 Writer 必须具有非空的 batch
// 将多个连续的写请求合并成一个批处理组
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld(); // 确保持有锁
  assert(!writers_.empty()); // Writer队列不应为空
  Writer* first = writers_.front(); // 获取队列头部的Writer
  WriteBatch* result = first->batch; // 初始结果为第一个Writer的批处理
  assert(result != nullptr); // 第一个Writer的批处理不应为空

  size_t size = WriteBatchInternal::ByteSize(first->batch); // 计算第一个批处理的大小

  // 允许组增长到最大大小，但如果原始写入很小，
  // 则限制增长，以免过多地减慢小型写入的速度。
  size_t max_size = 1 << 20; // 最大1MB
  if (size <= (128 << 10)) { // 如果原始大小小于等于128KB
    max_size = size + (128 << 10); // 最大大小限制为 原始大小 + 128KB
  }

  *last_writer = first; // 初始化最后一个Writer为第一个
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // 跳过 "first"
  for (; iter != writers_.end(); ++iter) { // 遍历队列中后续的Writer
    Writer* w = *iter;
    if (w->sync && !first->sync) { // 如果当前Writer要求同步，而第一个Writer不要求同步
      // 不要将同步写入包含到由非同步写入处理的批处理中。
      break; // 停止合并
    }

    if (w->batch != nullptr) { // 如果当前Writer有批处理数据
      size += WriteBatchInternal::ByteSize(w->batch); // 累加大小
      if (size > max_size) { // 如果超过最大大小限制
        // 不要让批处理太大
        break; // 停止合并
      }

      // 追加到 *result
      if (result == first->batch) { // 如果result仍然是第一个Writer的原始batch
        // 切换到临时批处理，而不是干扰调用者的批处理
        result = tmp_batch_; // 使用DBImpl的临时批处理对象
        assert(WriteBatchInternal::Count(result) == 0); // 临时批处理应为空
        WriteBatchInternal::Append(result, first->batch); // 将第一个批处理追加到临时批处理
      }
      WriteBatchInternal::Append(result, w->batch); // 将当前Writer的批处理追加到结果批处理
    }
    *last_writer = w; // 更新最后一个Writer
  }
  return result; // 返回合并后的批处理 (可能是原始的，也可能是tmp_batch_)
}

// 要求：持有 mutex_
// 要求：此线程当前位于写入程序队列的前面
// 为写入操作腾出空间，可能包括切换MemTable、等待Compaction或延迟写入
Status DBImpl::MakeRoomForWrite(bool force) { // force: 是否强制切换MemTable (即使当前MemTable未满)
  mutex_.AssertHeld(); // 确保持有锁
  assert(!writers_.empty()); // Writer队列不应为空
  bool allow_delay = !force; // 如果不是强制，则允许延迟
  Status s;
  while (true) { // 循环直到有足够空间或发生错误
    if (!bg_error_.ok()) { // 如果存在后台错误
      // 产生先前的错误
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >= // 如果允许延迟且Level-0文件数达到软限制
                                  config::kL0_SlowdownWritesTrigger) {
      // 我们即将达到 L0 文件的硬限制。与其在达到硬限制时
      // 将单个写入延迟几秒钟，不如开始将每个单独的写入延迟 1 毫秒，
      // 以减少延迟差异。此外，此延迟会将一些 CPU 交给 compaction 线程，
      // 以防它与写入程序共享同一核心。
      mutex_.Unlock(); // 解锁，执行休眠
      env_->SleepForMicroseconds(1000); // 休眠1毫秒
      allow_delay = false;  // 单个写入操作最多只延迟一次
      mutex_.Lock(); // 重新加锁
    } else if (!force && // 如果不是强制切换
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) { // 且当前MemTable有空间
      // 当前 memtable 中有空间
      break; // 空间足够，退出循环
    } else if (imm_ != nullptr) { // 如果存在不可变MemTable (正在等待compact)
      // 我们已经填满了当前的 memtable，但是前一个 memtable 仍在进行 compaction，所以我们等待。
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait(); // 等待后台工作完成 (主要是imm_的compaction)
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) { // 如果Level-0文件数达到硬限制
      // Level-0 文件过多。
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait(); // 等待后台工作完成 (主要是Level-0的compaction)
    } else {
      // 尝试切换到新的 memtable 并触发旧 memtable 的 compaction
      assert(versions_->PrevLogNumber() == 0); // 此时不应有PrevLogNumber (旧版兼容逻辑)
      uint64_t new_log_number = versions_->NewFileNumber(); // 获取新的日志文件编号
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile); // 创建新的日志文件
      if (!s.ok()) {
        // 避免在紧密循环中耗尽文件号空间。
        versions_->ReuseFileNumber(new_log_number); // 重用文件号
        break; // 创建日志文件失败，退出
      }

      delete log_; // 删除旧的日志写入器

      s = logfile_->Close(); // 关闭旧的日志文件
      if (!s.ok()) {
        // 我们可能丢失了写入到前一个日志文件的一些数据。
        // 无论如何都要切换到新的日志文件，但将其记录为后台错误，
        // 以便我们不再尝试任何写入。
        //
        // 我们或许可以尝试保存与日志文件对应的 memtable，
        // 如果可行，则抑制错误，但这会在关键代码路径中增加更多复杂性。
        RecordBackgroundError(s); // 记录后台错误
      }
      delete logfile_; // 删除旧的日志文件对象

      logfile_ = lfile; // 更新为新的日志文件
      logfile_number_ = new_log_number; // 更新日志文件编号
      log_ = new log::Writer(lfile); // 创建新的日志写入器
      imm_ = mem_; // 将当前MemTable设为不可变MemTable
      has_imm_.store(true, std::memory_order_release); // 标记存在不可变MemTable
      mem_ = new MemTable(internal_comparator_); // 创建新的当前MemTable
      mem_->Ref(); // 增加引用计数
      force = false;  // 如果有空间，则不要强制进行另一次 compaction
      MaybeScheduleCompaction(); // 尝试调度compaction (因为imm_已存在)
    }
  }
  return s;
}

// 获取数据库属性信息
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear(); // 清空输出字符串

  MutexLock l(&mutex_); // 加锁
  Slice in = property; // 输入的属性名
  Slice prefix("leveldb."); // 属性名前缀
  if (!in.starts_with(prefix)) return false; // 如果不以指定前缀开头，则不是leveldb属性
  in.remove_prefix(prefix.size()); // 移除前缀

  if (in.starts_with("num-files-at-level")) { // 获取某一层级的文件数量
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty(); // 解析层级号
    if (!ok || level >= config::kNumLevels) { // 层级号无效
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level))); // 获取文件数并格式化输出
      *value = buf;
      return true;
    }
  } else if (in == "stats") { // 获取数据库统计信息
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) { // 遍历所有层级
      int files = versions_->NumLevelFiles(level); // 该层级文件数
      if (stats_[level].micros > 0 || files > 0) { // 如果该层级有统计信息或文件
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0, // 大小(MB)
                      stats_[level].micros / 1e6, // Compaction时间(秒)
                      stats_[level].bytes_read / 1048576.0, // 读取字节数(MB)
                      stats_[level].bytes_written / 1048576.0); // 写入字节数(MB)
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") { // 获取SSTable的调试字符串
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") { // 获取近似内存使用量
    size_t total_usage = options_.block_cache->TotalCharge(); // 块缓存使用量
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage(); // MemTable使用量
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage(); // 不可变MemTable使用量
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false; // 未知属性
}

// 获取指定键范围的大致大小
void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): 更好的实现
  MutexLock l(&mutex_); // 加锁
  Version* v = versions_->current(); // 获取当前版本
  v->Ref(); // 增加引用计数

  for (int i = 0; i < n; i++) { // 遍历所有范围
    // 将 user_key 转换为相应的 internal key。
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek); // 范围起始键
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek); // 范围结束键
    uint64_t start = versions_->ApproximateOffsetOf(v, k1); // 获取起始键的近似偏移量
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2); // 获取结束键的近似偏移量
    sizes[i] = (limit >= start ? limit - start : 0); // 计算范围大小
  }

  v->Unref(); // 减少引用计数
}

// DB 基类中 Put 操作的默认实现
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch; // 创建一个写批处理
  batch.Put(key, value); // 将Put操作添加到批处理
  return Write(opt, &batch); // 调用DB的Write方法执行批处理
}

// DB 基类中 Delete 操作的默认实现
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch; // 创建一个写批处理
  batch.Delete(key); // 将Delete操作添加到批处理
  return Write(opt, &batch); // 调用DB的Write方法执行批处理
}

DB::~DB() = default; // DB的析构函数 (默认实现)

// 打开或创建数据库的静态工厂方法
Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr; // 初始化输出参数

  DBImpl* impl = new DBImpl(options, dbname); // 创建DBImpl实例
  impl->mutex_.Lock(); // 加锁
  VersionEdit edit;
  // Recover 处理 create_if_missing, error_if_exists
  bool save_manifest = false; // 是否需要保存MANIFEST
  Status s = impl->Recover(&edit, &save_manifest); // 执行恢复过程
  if (s.ok() && impl->mem_ == nullptr) { // 如果恢复成功且MemTable为空 (例如，全新数据库或所有日志都已刷盘)
    // 创建新的日志和相应的 memtable。
    uint64_t new_log_number = impl->versions_->NewFileNumber(); // 获取新的日志文件编号
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number), // 创建新的日志文件
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number); // 在版本编辑中设置新的日志号
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile); // 创建日志写入器
      impl->mem_ = new MemTable(impl->internal_comparator_); // 创建新的MemTable
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) { // 如果恢复成功且需要保存MANIFEST (例如，恢复过程中有日志被刷盘)
    edit.SetPrevLogNumber(0);  // 恢复后不再需要旧的日志。
    edit.SetLogNumber(impl->logfile_number_); // 设置当前日志号
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_); // 应用版本编辑并记录到MANIFEST
  }
  if (s.ok()) { // 如果一切顺利
    impl->RemoveObsoleteFiles(); // 删除过时的文件
    impl->MaybeScheduleCompaction(); // 尝试调度compaction
  }
  impl->mutex_.Unlock(); // 解锁
  if (s.ok()) { // 如果成功打开/创建数据库
    assert(impl->mem_ != nullptr); // MemTable不应为空
    *dbptr = impl; // 返回DBImpl实例
  } else {
    delete impl; // 如果失败，则删除DBImpl实例
  }
  return s;
}

Snapshot::~Snapshot() = default; // Snapshot的析构函数 (默认实现)

// 销毁指定名称的数据库
Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env; // 获取环境对象
  std::vector<std::string> filenames; // 用于存储数据库目录下的文件名
  Status result = env->GetChildren(dbname, &filenames); // 获取文件名列表
  if (!result.ok()) {
    // 忽略错误，以防目录不存在
    return Status::OK();
  }

  FileLock* lock; // 文件锁
  const std::string lockname = LockFileName(dbname); // 锁文件名
  result = env->LockFile(lockname, &lock); // 尝试锁定数据库
  if (result.ok()) { // 如果锁定成功
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) { // 遍历所有文件
      if (ParseFileName(filenames[i], &number, &type) && // 解析文件名
          type != kDBLockFile) {  // 锁文件将在最后删除
        Status del = env->RemoveFile(dbname + "/" + filenames[i]); // 删除文件
        if (result.ok() && !del.ok()) { // 如果之前的result是ok，但删除失败，则更新result
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // 忽略错误，因为状态已经消失
    env->RemoveFile(lockname); // 删除锁文件
    env->RemoveDir(dbname);  // 忽略错误，以防目录包含其他文件
  }
  return result;
}

}  // namespace leveldb