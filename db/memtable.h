// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_ // 防止头文件被重复包含
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string> // 引入 std::string 类型

#include "db/dbformat.h"   // 包含数据库格式定义，如 InternalKey, SequenceNumber, ValueType 等
#include "db/skiplist.h"   // 包含跳表数据结构的实现
#include "leveldb/db.h"    // 包含 LevelDB 公共 API 定义，如 Iterator, Status 等
#include "util/arena.h"    // 包含 Arena 内存分配器的实现

namespace leveldb { // 定义 leveldb 命名空间

class InternalKeyComparator; // 前向声明内部键比较器类
class MemTableIterator;      // 前向声明 MemTable 迭代器类

// MemTable 是内存中的数据结构，用于存储最近的写操作。
// 它使用跳表来组织数据，以便进行高效的查找和有序遍历。
// MemTable 是线程安全的，但写操作需要外部同步。
class MemTable {
 public:
  // MemTable 使用引用计数。
  // 初始引用计数为零，调用者必须至少调用一次 Ref()。
  explicit MemTable(const InternalKeyComparator& comparator); // 构造函数，接收一个内部键比较器

  MemTable(const MemTable&) = delete; // 禁止拷贝构造
  MemTable& operator=(const MemTable&) = delete; // 禁止拷贝赋值

  // 增加引用计数。
  void Ref() { ++refs_; }

  // 减少引用计数。如果引用计数降至零，则删除对象。
  void Unref() {
    --refs_;
    assert(refs_ >= 0); // 断言引用计数不为负
    if (refs_ <= 0) {   // 如果引用计数小于等于0
      delete this;      // 删除当前对象
    }
  }

  // 返回此数据结构正在使用的数据字节数的估计值。
  // 在 MemTable 被修改时调用此方法是安全的。
  size_t ApproximateMemoryUsage();

  // 返回一个迭代器，该迭代器产生 memtable 的内容。
  //
  // 调用者必须确保在返回的迭代器存活期间，底层的 MemTable 保持存活。
  // 此迭代器返回的键是由 db/format.{h,cc} 模块中的 AppendInternalKey 编码的内部键。
  Iterator* NewIterator();

  // 向 memtable 中添加一个条目，该条目将键映射到值，
  // 具有指定的序列号和指定的类型。
  // 通常，如果 type==kTypeDeletion，则 value 为空。
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);

  // 如果 memtable 包含键的值，则将其存储在 *value 中并返回 true。
  // 如果 memtable 包含键的删除标记，则在 *status 中存储 NotFound() 错误并返回 true。
  // 否则，返回 false。
  bool Get(const LookupKey& key, std::string* value, Status* s);

 private:
  // 声明友元类，允许它们访问 MemTable 的私有成员
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  // 用于跳表内部的键比较器结构体
  struct KeyComparator {
    const InternalKeyComparator comparator; // 内部键比较器对象
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {} // 构造函数
    // 重载 operator() 以比较两个 char* 类型的键
    // 这两个 char* 指向编码后的内部键
    int operator()(const char* a, const char* b) const;
  };

  // 定义 Table 类型为 SkipList，键类型为 const char*，比较器类型为 KeyComparator
  typedef SkipList<const char*, KeyComparator> Table;

  // 析构函数声明为私有，因为只能通过 Unref() 来删除对象
  ~MemTable();

  KeyComparator comparator_; // 键比较器实例
  int refs_;                 // 引用计数
  Arena arena_;              // 用于内存分配的 Arena 对象
  Table table_;              // 底层存储数据的跳表实例
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_