// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// 前向声明
class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

// DB 接口的实现类
class DBImpl : public DB {
 public:
  // 构造函数
  DBImpl(const Options& options, const std::string& dbname);

  // 禁止拷贝构造和赋值操作
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  // 析构函数
  ~DBImpl() override;

  // DB 接口的实现
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override; // 写入键值对
  Status Delete(const WriteOptions&, const Slice& key) override; // 删除键
  Status Write(const WriteOptions& options, WriteBatch* updates) override; // 批量写入
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override; // 读取键对应的值
  Iterator* NewIterator(const ReadOptions&) override; // 创建迭代器
  const Snapshot* GetSnapshot() override; // 获取快照
  void ReleaseSnapshot(const Snapshot* snapshot) override; // 释放快照
  bool GetProperty(const Slice& property, std::string* value) override; // 获取数据库属性
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override; // 获取指定范围的大致大小
  void CompactRange(const Slice* begin, const Slice* end) override; // 对指定范围进行 Compaction

  // 额外的测试方法，不在公共 DB 接口中

  // 对指定层级中与 [*begin,*end] 重叠的文件进行 Compaction
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // 强制对当前 memtable 内容进行 Compaction
  Status TEST_CompactMemTable();

  // 返回一个遍历数据库当前状态的内部迭代器。
  // 此迭代器的键是内部键 (参见 format.h)。
  // 返回的迭代器在不再需要时应被删除。
  Iterator* TEST_NewInternalIterator();

  // 返回对于层级 >= 1 的任何文件，在下一层级的最大重叠数据量 (字节)。
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // 记录在指定内部键处读取的字节样本。
  // 大约每 config::kReadBytesPeriod 字节进行一次采样。
  void RecordReadSample(Slice key);

 private:
  friend class DB; // 声明 DB 类为友元类
  struct CompactionState; // Compaction 状态结构体
  struct Writer; // 写入者结构体

  // 手动 Compaction 的信息
  struct ManualCompaction {
    int level; // Compaction 的层级
    bool done; // 是否完成
    const InternalKey* begin;  // null 表示键范围的开始
    const InternalKey* end;    // null 表示键范围的结束
    InternalKey tmp_storage;   // 用于跟踪 Compaction 进度
  };

  // 每层 Compaction 的统计信息。stats_[level] 存储为指定 "level" 生成数据的 Compaction 统计。
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {} // 构造函数

    // 添加另一个 CompactionStats 的数据
    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros; // Compaction 耗时 (微秒)
    int64_t bytes_read; // 读取的字节数
    int64_t bytes_written; // 写入的字节数
  };

  // 创建内部迭代器
  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot, // 最新的快照序列号
                                uint32_t* seed); // 迭代器种子

  // 创建新的数据库
  Status NewDB();

  // 从持久存储中恢复描述符。可能需要大量工作来恢复最近记录的更新。
  // 对描述符所做的任何更改都将添加到 *edit 中。
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 根据 paranoid_checks 选项决定是否忽略错误
  void MaybeIgnoreError(Status* s) const;

  // 删除任何不需要的文件和过时的内存条目。
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 将内存中的写缓冲区 Compact 到磁盘。如果成功，则切换到新的日志文件/memtable 并写入新的描述符。
  // 错误记录在 bg_error_ 中。
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 从指定的日志文件恢复数据
  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 将 MemTable 的内容写入 Level-0 的 SSTable 文件
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 为写入操作腾出空间 (如果需要，即使有空间也进行 compact)
  Status MakeRoomForWrite(bool force)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁
  // 构建写入批处理组
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 记录后台发生的错误
  void RecordBackgroundError(const Status& s);

  // 尝试调度后台 Compaction
  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁
  // 后台工作的静态包装函数
  static void BGWork(void* db);
  // 后台调用的主函数
  void BackgroundCall();
  // 执行后台 Compaction 的核心逻辑
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁
  // 清理 CompactionState 对象
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁
  // 执行 Compaction 的主要工作流程
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 为 Compaction 打开输出文件
  Status OpenCompactionOutputFile(CompactionState* compact);
  // 完成 Compaction 输出文件的写入
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  // 安装 Compaction 结果 (更新 VersionSet)
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_); // 执行期间需要持有 mutex_ 锁

  // 获取用户比较器
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // 构造后为常量的成员变量
  Env* const env_; // 环境对象
  const InternalKeyComparator internal_comparator_; // 内部键比较器
  const InternalFilterPolicy internal_filter_policy_; // 内部过滤器策略
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_; // 是否拥有 info_log 的所有权
  const bool owns_cache_; // 是否拥有 cache 的所有权
  const std::string dbname_; // 数据库名称

  // table_cache_ 提供其自身的同步机制
  TableCache* const table_cache_;

  // 持久 DB 状态的锁。如果成功获取，则非空。
  FileLock* db_lock_;

  // 以下状态受 mutex_ 保护
  port::Mutex mutex_; // 互斥锁
  std::atomic<bool> shutting_down_; // 是否正在关闭
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_); // 后台工作完成信号，受 mutex_ 保护
  MemTable* mem_ GUARDED_BY(mutex_); // 当前的 memtable，受 mutex_ 保护 (实际实现中 mem_ 的访问有时在锁外，但其切换受锁保护)
  MemTable* imm_ GUARDED_BY(mutex_);  // 正在进行 Compaction 的 Memtable，受 mutex_ 保护
  std::atomic<bool> has_imm_;         // 以便后台线程可以检测到非空的 imm_
  WritableFile* logfile_ GUARDED_BY(mutex_); // 当前日志文件，受 mutex_ 保护 (实际实现中 logfile_ 的创建和切换受锁保护)
  uint64_t logfile_number_ GUARDED_BY(mutex_); // 当前日志文件编号，受 mutex_ 保护
  log::Writer* log_ GUARDED_BY(mutex_); // 日志写入器，受 mutex_ 保护 (实际实现中 log_ 的创建和切换受锁保护)
  uint32_t seed_ GUARDED_BY(mutex_);  // 用于采样的种子，受 mutex_ 保护

  // 写入者队列
  std::deque<Writer*> writers_ GUARDED_BY(mutex_); // 受 mutex_ 保护
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_); // 用于合并写入的临时批处理，受 mutex_ 保护

  SnapshotList snapshots_ GUARDED_BY(mutex_); // 快照列表，受 mutex_ 保护

  // 表文件集合，用于保护正在进行的 Compaction 所涉及的文件不被删除。
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_); // 受 mutex_ 保护

  // 是否已调度或正在运行后台 Compaction？
  bool background_compaction_scheduled_ GUARDED_BY(mutex_); // 受 mutex_ 保护

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_); // 手动 Compaction 请求，受 mutex_ 保护

  VersionSet* const versions_ GUARDED_BY(mutex_); // 版本集，受 mutex_ 保护

  // 在 paranoid 模式下是否遇到后台错误？
  Status bg_error_ GUARDED_BY(mutex_); // 后台错误状态，受 mutex_ 保护

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_); // 各层 Compaction 统计信息，受 mutex_ 保护
};

// 规范化数据库选项。如果 result.info_log 不等于 src.info_log，调用者应删除 result.info_log。
Options SanitizeOptions(const std::string& db, // 数据库名称
                        const InternalKeyComparator* icmp, // 内部键比较器
                        const InternalFilterPolicy* ipolicy, // 内部过滤器策略
                        const Options& src); // 源 Options 对象

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_