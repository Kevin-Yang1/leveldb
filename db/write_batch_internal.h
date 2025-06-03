// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_ // 防止头文件重复包含
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"           // 引入数据库格式定义，例如 SequenceNumber
#include "leveldb/write_batch.h" // 引入 WriteBatch 公共接口定义

namespace leveldb {

class MemTable; // 前向声明 MemTable 类

// WriteBatchInternal 提供了一些静态方法，用于操作 WriteBatch。
// 这些方法不希望出现在公共的 WriteBatch 接口中，因此放在这个内部类里。
class WriteBatchInternal {
 public:
  // 返回批处理中的条目数量。
  static int Count(const WriteBatch* batch);

  // 设置批处理中的条目数量。
  static void SetCount(WriteBatch* batch, int n);

  // 返回此批处理起始的序列号。
  static SequenceNumber Sequence(const WriteBatch* batch);

  // 将指定的数字存储为此批处理起始的序列号。
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // 返回批处理内容的 Slice 视图。
  // 直接访问 WriteBatch 的内部表示 rep_。
  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  // 返回批处理内容的总字节大小。
  // 直接访问 WriteBatch 内部表示 rep_ 的大小。
  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  // 设置批处理的内容。
  // 用给定的 Slice 数据替换 WriteBatch 的内部表示 rep_。
  static void SetContents(WriteBatch* batch, const Slice& contents);

  // 将批处理中的所有操作插入到指定的 MemTable 中。
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  // 将源批处理 (src) 的内容追加到目标批处理 (dst) 中。
  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_