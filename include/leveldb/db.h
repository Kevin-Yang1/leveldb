// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_DB_H_ // 防止头文件重复包含
#define STORAGE_LEVELDB_INCLUDE_DB_H_

#include <cstdint> // 引入 C++ 标准整数类型，如 uint64_t
#include <cstdio>  // 引入 C 标准输入输出库，例如 FILE (虽然在此文件中未直接使用，但可能是为了某些依赖)

#include "leveldb/export.h"   // 引入导出宏定义，用于控制符号的可见性
#include "leveldb/iterator.h" // 引入迭代器接口定义
#include "leveldb/options.h"  // 引入数据库选项定义

namespace leveldb {

// 如果更改这些版本号，请同时更新 CMakeLists.txt 文件
static const int kMajorVersion = 1; // LevelDB 主版本号
static const int kMinorVersion = 23; // LevelDB 次版本号

struct Options;     // 前向声明 Options 结构体
struct ReadOptions; // 前向声明 ReadOptions 结构体
struct WriteOptions; // 前向声明 WriteOptions 结构体
class WriteBatch;   // 前向声明 WriteBatch 类

// 表示数据库特定状态的抽象句柄。
// Snapshot 是一个不可变对象，因此可以从多个线程安全地访问，无需任何外部同步。
class LEVELDB_EXPORT Snapshot {
 protected:
  virtual ~Snapshot(); // 虚析构函数，允许派生类正确清理资源
};

// 表示一个键的范围
struct LEVELDB_EXPORT Range {
  Range() = default; // 默认构造函数
  Range(const Slice& s, const Slice& l) : start(s), limit(l) {} // 使用起始和结束 Slice 构造

  Slice start;  // 范围的起始键 (包含在范围内)
  Slice limit;  // 范围的结束键 (不包含在范围内)
};

// DB 是一个从键到值的持久化有序映射。
// DB 可以从多个线程并发访问，无需任何外部同步。
class LEVELDB_EXPORT DB {
 public:
  // 使用指定的 "name" 打开数据库。
  // 成功时，在 *dbptr 中存储一个指向堆分配数据库的指针，并返回 OK。
  // 失败时，在 *dbptr 中存储 nullptr，并返回一个非 OK 状态。
  // 调用者在不再需要 *dbptr 时应将其删除。
  static Status Open(const Options& options, const std::string& name,
                     DB** dbptr);

  DB() = default; // 默认构造函数

  DB(const DB&) = delete; // 禁止拷贝构造
  DB& operator=(const DB&) = delete; // 禁止拷贝赋值

  virtual ~DB(); // 虚析构函数，确保派生类的析构函数被调用

  // 将 "key" 对应的数据库条目设置为 "value"。成功时返回 OK，失败时返回非 OK 状态。
  // 注意：考虑设置 options.sync = true 以确保数据持久化。
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;

  // 删除 "key" 对应的数据库条目 (如果存在)。成功时返回 OK，失败时返回非 OK 状态。
  // 如果 "key" 不存在于数据库中，这不 يعتبر错误。
  // 注意：考虑设置 options.sync = true。
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // 将指定的更新应用于数据库。
  // 成功时返回 OK，失败时返回非 OK 状态。
  // 注意：考虑设置 options.sync = true。
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

  // 如果数据库包含 "key" 的条目，则将对应的值存储在 *value 中并返回 OK。
  //
  // 如果 "key" 没有条目，则保持 *value 不变，并返回一个 Status::IsNotFound() 为 true 的状态。
  //
  // 发生错误时可能返回其他 Status。
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  // 返回一个堆分配的迭代器，用于遍历数据库内容。
  // NewIterator() 的结果初始时是无效的 (调用者在使用迭代器之前必须调用其 Seek 方法之一)。
  //
  // 调用者在不再需要迭代器时应将其删除。
  // 返回的迭代器应在此 db 被删除之前删除。
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;

  // 返回当前数据库状态的句柄。使用此句柄创建的迭代器将观察到当前数据库状态的稳定快照。
  // 调用者在不再需要快照时必须调用 ReleaseSnapshot(result)。
  virtual const Snapshot* GetSnapshot() = 0;

  // 释放先前获取的快照。调用者在此调用之后不得再使用 "snapshot"。
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  // DB 实现可以通过此方法导出有关其状态的属性。
  // 如果 "property" 是此 DB 实现理解的有效属性，则用其当前值填充 "*value" 并返回 true。否则返回 false。
  //
  //
  // 有效的属性名称包括：
  //
  //  "leveldb.num-files-at-level<N>" - 返回级别 <N> 的文件数量，
  //     其中 <N> 是级别号的 ASCII 表示 (例如 "0")。
  //  "leveldb.stats" - 返回一个多行字符串，描述有关数据库内部操作的统计信息。
  //  "leveldb.sstables" - 返回一个多行字符串，描述构成数据库内容的所有 sstable。
  //  "leveldb.approximate-memory-usage" - 返回数据库正在使用的大致内存字节数。
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;

  // 对于 [0,n-1] 中的每个 i，在 "sizes[i]" 中存储 "[range[i].start .. range[i].limit)" 中键所使用的近似文件系统空间。
  //
  // 注意，返回的大小衡量的是文件系统空间使用情况，因此如果用户数据压缩了十倍，返回的大小将是相应用户数据大小的十分之一。
  //
  // 结果可能不包括最近写入数据的大小。
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) = 0;

  // 压缩键范围 [*begin,*end] 的底层存储。
  // 特别是，已删除和已覆盖的版本将被丢弃，并且数据将被重新排列以减少访问数据所需的操作成本。
  // 此操作通常只应由了解底层实现的用户调用。
  //
  // begin==nullptr 被视为空指针，代表数据库中所有键之前的键。
  // end==nullptr 被视为空指针，代表数据库中所有键之后的键。
  // 因此，以下调用将压缩整个数据库：
  //    db->CompactRange(nullptr, nullptr);
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
};

// 销毁指定数据库的内容。
// 使用此方法时要非常小心。
//
// 注意：为了向后兼容，如果 DestroyDB 无法列出数据库文件，仍将返回 Status::OK()，从而掩盖此失败。
LEVELDB_EXPORT Status DestroyDB(const std::string& name,
                                const Options& options);

// 如果无法打开数据库，您可以尝试调用此方法以尽可能多地恢复数据库内容。
// 可能会丢失一些数据，因此在对包含重要信息的数据库调用此函数时要小心。
LEVELDB_EXPORT Status RepairDB(const std::string& dbname,
                               const Options& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_H_