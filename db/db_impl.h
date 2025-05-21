// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic> // 原子操作库
#include <deque>  // 双端队列库
#include <set>    // 集合库
#include <string> // 字符串库

#include "db/dbformat.h"      // 数据库格式定义
#include "db/log_writer.h"    // 日志写入器
#include "db/snapshot.h"      // 快照实现
#include "leveldb/db.h"       // LevelDB 公共接口
#include "leveldb/env.h"      // LevelDB 环境抽象
#include "port/port.h"        // 平台相关的可移植性代码
#include "port/thread_annotations.h" // 线程安全注解

namespace leveldb {

// 前向声明
class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

// DBImpl 是 DB 接口的具体实现类
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
  // 插入或更新键值对
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  // 删除键
  Status Delete(const WriteOptions&, const Slice& key) override;
  // 批量写入操作
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  // 获取键对应的值
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  // 创建一个新的迭代器
  Iterator* NewIterator(const ReadOptions&) override;
  // 获取当前数据库状态的快照
  const Snapshot* GetSnapshot() override;
  // 释放之前获取的快照
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  // 获取数据库的属性信息
  bool GetProperty(const Slice& property, std::string* value) override;
  // 获取指定键范围的大致大小
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  // 手动触发对指定键范围的 Compaction
  void CompactRange(const Slice* begin, const Slice* end) override;

  // 额外的测试方法 (不在公共 DB 接口中)

  // 对指定层级中与 [*begin,*end] 重叠的文件进行 Compaction
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // 强制将当前 MemTable 的内容进行 Compaction
  Status TEST_CompactMemTable();

  // 返回一个遍历数据库当前状态的内部迭代器。
  // 此迭代器的键是内部键 (参见 format.h)。
  // 返回的迭代器在不再需要时应被删除。
  Iterator* TEST_NewInternalIterator();

  // 返回在层级 >= 1 的任何文件中，下一层级的最大重叠数据量 (字节)。
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // 记录在指定内部键处读取的字节样本。
  // 大约每 config::kReadBytesPeriod 字节进行一次采样。
  void RecordReadSample(Slice key);

 private:
  friend class DB; // 允许 DB 类访问私有成员 (例如 DB::Open)
  struct CompactionState; // Compaction 过程中的状态信息
  struct Writer;          // 代表一个写入请求

  // 手动 Compaction 的信息
  struct ManualCompaction {
    int level;                // Compaction 的目标层级
    bool done;                // Compaction 是否已完成
    const InternalKey* begin; // 起始内部键 (nullptr 表示从键范围的开始)
    const InternalKey* end;   // 结束内部键 (nullptr 表示到键范围的结束)
    InternalKey tmp_storage;  // 用于跟踪 Compaction 进度
  };

  // 每个层级的 Compaction 统计信息。stats_[level] 存储了为指定 "level" 生成数据的 Compaction 的统计。
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    // 累加统计数据
    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;        // Compaction 耗时 (微秒)
    int64_t bytes_read;    // Compaction 读取的字节数
    int64_t bytes_written; // Compaction 写入的字节数
  };

  // 创建一个内部迭代器，合并 MemTable、Immutable MemTable 和 SSTable
  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot, // 输出：最新的序列号
                                uint32_t* seed);                 // 输出：迭代器种子

  // 创建一个新的数据库 (初始化 MANIFEST 和 CURRENT 文件)
  Status NewDB();

  // 从持久化存储中恢复描述符。可能需要大量工作来恢复最近记录的更新。
  // 任何对描述符的更改都会添加到 *edit 中。
  // 调用此函数前必须持有 mutex_ 锁
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 根据 paranoid_checks 选项决定是否忽略错误
  void MaybeIgnoreError(Status* s) const;

  // 删除任何不需要的文件和过时的内存条目。
  // 调用此函数前必须持有 mutex_ 锁
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 将内存中的写缓冲区 Compact 到磁盘。如果成功，则切换到新的日志文件/MemTable 并写入新的描述符。
  // 错误记录在 bg_error_ 中。
  // 调用此函数前必须持有 mutex_ 锁
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 从指定的日志文件恢复数据
  // 调用此函数前必须持有 mutex_ 锁
  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 将 MemTable 的内容写入 Level-0 的 SSTable 文件
  // 调用此函数前必须持有 mutex_ 锁
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 为写入操作腾出空间 (如果需要，即使有空间也进行 Compaction)
  // 调用此函数前必须持有 mutex_ 锁
  Status MakeRoomForWrite(bool force)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // 将多个写入请求合并成一个批处理组
  // 调用此函数前必须持有 mutex_ 锁
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 记录后台发生的错误
  void RecordBackgroundError(const Status& s);

  // 尝试调度一次后台 Compaction
  // 调用此函数前必须持有 mutex_ 锁
  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // 后台工作线程的入口函数 (静态成员)
  static void BGWork(void* db);
  // 后台 Compaction 的主要逻辑调用
  void BackgroundCall();
  // 执行后台 Compaction 的核心逻辑
  // 调用此函数前必须持有 mutex_ 锁
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // 清理 CompactionState 相关资源
  // 调用此函数前必须持有 mutex_ 锁
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // 执行 Compaction 的实际工作 (合并 SSTable)
  // 调用此函数前必须持有 mutex_ 锁
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 为 Compaction 创建一个新的输出 SSTable 文件
  Status OpenCompactionOutputFile(CompactionState* compact);
  // 完成一个 Compaction 输出 SSTable 文件的写入
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  // 将 Compaction 的结果应用到 VersionSet
  // 调用此函数前必须持有 mutex_ 锁
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 获取用户定义的比较器
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // 构造后为常量的成员变量
  Env* const env_; // 环境对象 (例如文件系统操作)
  const InternalKeyComparator internal_comparator_; // 内部键比较器
  const InternalFilterPolicy internal_filter_policy_; // 内部过滤器策略
  const Options options_;  // 数据库选项 (options_.comparator == &internal_comparator_)
  const bool owns_info_log_; // DBImpl 是否拥有 info_log 的所有权
  const bool owns_cache_;    // DBImpl 是否拥有 block_cache 的所有权
  const std::string dbname_; // 数据库名称

  // table_cache_ 提供其自身的同步机制
  TableCache* const table_cache_; // 表缓存，用于缓存打开的 SSTable

  // 持久化数据库状态的锁。如果成功获取则非空。
  FileLock* db_lock_; // 数据库锁文件句柄

  // 以下状态受 mutex_ 保护
  port::Mutex mutex_; // 互斥锁，用于保护共享状态
  std::atomic<bool> shutting_down_; // 数据库是否正在关闭的原子标志
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_); // 后台工作完成的条件变量
  MemTable* mem_ GUARDED_BY(mutex_); // 当前的可写 MemTable
  MemTable* imm_ GUARDED_BY(mutex_);  // 正在被 Compact 的 MemTable (不可变)
  std::atomic<bool> has_imm_;         // 用于后台线程检测 imm_ 是否非空
  WritableFile* logfile_ GUARDED_BY(mutex_); // 当前日志文件的可写文件对象
  uint64_t logfile_number_ GUARDED_BY(mutex_); // 当前日志文件的编号
  log::Writer* log_ GUARDED_BY(mutex_); // 日志写入器对象
  uint32_t seed_ GUARDED_BY(mutex_);  // 用于采样的种子

  // 写入者队列，受 mutex_ 保护
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  // 用于合并多个写入操作的临时 WriteBatch，受 mutex_ 保护
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  // 数据库快照列表，受 mutex_ 保护
  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // 表文件集合，用于保护正在进行的 Compaction 所涉及的文件不被删除，受 mutex_ 保护
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // 后台 Compaction 是否已被调度或正在运行的标志，受 mutex_ 保护
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  // 指向手动 Compaction 请求的指针，受 mutex_ 保护
  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  // VersionSet 对象，管理数据库的版本信息，其本身包含同步机制，但访问 versions_ 指针本身需要 mutex_ 保护
  VersionSet* const versions_ GUARDED_BY(mutex_);

  // 在偏执模式下是否遇到后台错误的标志，受 mutex_ 保护
  Status bg_error_ GUARDED_BY(mutex_);

  // 各个层级的 Compaction 统计信息，受 mutex_ 保护
  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// 规范化数据库选项。如果返回的 result.info_log 不等于 src.info_log，
// 调用者需要负责删除 result.info_log。
Options SanitizeOptions(const std::string& db, // 数据库名称
                        const InternalKeyComparator* icmp, // 内部键比较器
                        const InternalFilterPolicy* ipolicy, // 内部过滤器策略
                        const Options& src); // 原始选项

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_