// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch 保存了一系列要原子性地应用于数据库的更新操作。
//
// 更新操作按照它们被添加到 WriteBatch 中的顺序应用。
// 例如，在写入以下批处理后，"key" 的值将是 "v3"：
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// 多个线程可以在没有外部同步的情况下调用 WriteBatch 上的 const 方法，
// 但是如果任何线程可能调用非 const 方法，则所有访问相同 WriteBatch 的线程都必须使用外部同步。

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string> // 引入 C++ 标准库字符串

#include "leveldb/export.h" // 引入导出宏定义
#include "leveldb/status.h" // 引入 Status 类定义

namespace leveldb {

class Slice; // 前向声明 Slice 类

// WriteBatch 类用于批量写入操作
class LEVELDB_EXPORT WriteBatch {
 public:
  // Handler 类用于处理 WriteBatch 中的操作，通常在迭代时使用
  class LEVELDB_EXPORT Handler {
   public:
    virtual ~Handler(); // 虚析构函数
    // 处理 Put 操作的纯虚函数
    virtual void Put(const Slice& key, const Slice& value) = 0;
    // 处理 Delete 操作的纯虚函数
    virtual void Delete(const Slice& key) = 0;
  };

  WriteBatch(); // 默认构造函数

  // 允许拷贝构造和拷贝赋值
  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch(); // 析构函数

  // 在数据库中存储 "key" 到 "value" 的映射。
  void Put(const Slice& key, const Slice& value);

  // 如果数据库包含 "key" 的映射，则擦除它。否则什么也不做。
  void Delete(const Slice& key);

  // 清除此批处理中缓冲的所有更新。
  void Clear();

  // 此批处理引起的数据库更改的大致大小。
  //
  // 这个数字与实现细节相关，并且可能在不同版本之间发生变化。
  // 它主要用于 LevelDB 的使用指标。
  size_t ApproximateSize() const;

  // 将 "source" 中的操作复制到此批处理中。
  //
  // 这以 O(source 大小) 的时间运行。然而，其常数因子优于
  // 使用一个将操作复制到此批处理的 Handler 来迭代源批处理。
  void Append(const WriteBatch& source);

  // 支持迭代批处理内容的接口。
  // handler: 用于处理批处理中每个操作的回调对象
  // 返回值: 如果迭代成功，则返回 Status::OK()
  Status Iterate(Handler* handler) const;

 private:
  friend class WriteBatchInternal; // 声明 WriteBatchInternal 为友元类，允许其访问私有成员

  std::string rep_;  // 内部表示，其格式详见 write_batch.cc 中的注释
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_