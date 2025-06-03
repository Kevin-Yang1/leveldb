// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ 的内部存储格式如下：
//    sequence: fixed64  (8字节序列号)
//    count: fixed32     (4字节操作计数)
//    data: record[count] (实际的操作记录，数量由 count 指定)
// record 的格式如下 (根据操作类型不同)：
//    kTypeValue varstring varstring         (Put 操作：类型标记、键、值)
//    kTypeDeletion varstring                (Delete 操作：类型标记、键)
// varstring (变长字符串) 的格式如下：
//    len: varint32       (4字节变长整数，表示数据长度)
//    data: uint8[len]    (实际的字符串数据)

#include "leveldb/write_batch.h" // 引入 WriteBatch 头文件

#include "db/dbformat.h"           // 引入数据库格式定义
#include "db/memtable.h"           // 引入 MemTable 定义
#include "db/write_batch_internal.h" // 引入 WriteBatchInternal 定义，用于访问 WriteBatch 的内部成员
#include "leveldb/db.h"            // 引入 DB 接口定义
#include "util/coding.h"           // 引入编码/解码工具函数

namespace leveldb {

// WriteBatch 的头部包含一个 8 字节的序列号和一个 4 字节的计数。
static const size_t kHeader = 12; // 头部大小为 12 字节

// WriteBatch 默认构造函数
WriteBatch::WriteBatch() {
  Clear(); // 初始化时清空 rep_ 并设置头部空间
}

// WriteBatch 析构函数 (使用默认实现)
WriteBatch::~WriteBatch() = default;

// WriteBatch::Handler 析构函数 (使用默认实现)
WriteBatch::Handler::~Handler() = default;

// 清空 WriteBatch 中的所有操作，并重置头部空间
void WriteBatch::Clear() {
  rep_.clear();          // 清空内部字符串
  rep_.resize(kHeader); // 重新分配大小以容纳头部 (序列号和计数)
}

// 获取 WriteBatch 中数据的大致大小 (字节数)
size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

// 迭代 WriteBatch 中的所有操作，并通过 handler 处理每个操作
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_); // 将内部字符串 rep_ 包装成 Slice 以便处理
  if (input.size() < kHeader) { // 检查数据大小是否至少包含头部
    return Status::Corruption("malformed WriteBatch (too small)"); // 数据损坏：WriteBatch 太小
  }

  input.remove_prefix(kHeader); // 跳过头部 (序列号和计数)
  Slice key, value;             // 用于存储解析出的键和值
  int found = 0;                // 记录找到的操作数量
  while (!input.empty()) {      // 循环直到所有数据都被处理
    found++;
    char tag = input[0]; // 获取操作类型标记
    input.remove_prefix(1); // 移除类型标记
    switch (tag) {
      case kTypeValue: // Put 操作
        // 解析键和值 (都带有长度前缀)
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value); // 调用 handler 处理 Put 操作
        } else {
          return Status::Corruption("bad WriteBatch Put"); // 数据损坏：Put 操作格式错误
        }
        break;
      case kTypeDeletion: // Delete 操作
        // 解析键 (带有长度前缀)
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key); // 调用 handler 处理 Delete 操作
        } else {
          return Status::Corruption("bad WriteBatch Delete"); // 数据损坏：Delete 操作格式错误
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag"); // 数据损坏：未知的操作类型标记
    }
  }
  if (found != WriteBatchInternal::Count(this)) { // 检查解析出的操作数量是否与头部记录的计数一致
    return Status::Corruption("WriteBatch has wrong count"); // 数据损坏：WriteBatch 计数错误
  } else {
    return Status::OK(); // 迭代成功
  }
}

// 获取 WriteBatch 中的操作数量 (通过 WriteBatchInternal 访问)
int WriteBatchInternal::Count(const WriteBatch* b) {
  // 从 rep_ 的第 8 个字节开始解码 4 字节的计数值
  return DecodeFixed32(b->rep_.data() + 8);
}

// 设置 WriteBatch 中的操作数量 (通过 WriteBatchInternal 修改)
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  // 将计数值 n 编码为 4 字节并写入到 rep_ 的第 8 个字节开始的位置
  EncodeFixed32(&b->rep_[8], n);
}

// 获取 WriteBatch 的序列号 (通过 WriteBatchInternal 访问)
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  // 从 rep_ 的起始位置解码 8 字节的序列号
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

// 设置 WriteBatch 的序列号 (通过 WriteBatchInternal 修改)
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  // 将序列号 seq 编码为 8 字节并写入到 rep_ 的起始位置
  EncodeFixed64(&b->rep_[0], seq);
}

//向 WriteBatch 中添加一个 Put 操作
void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1); // 操作计数加 1
  rep_.push_back(static_cast<char>(kTypeValue)); // 添加 Put 操作的类型标记
  PutLengthPrefixedSlice(&rep_, key);            // 将键（带有长度前缀）追加到 rep_
  PutLengthPrefixedSlice(&rep_, value);          // 将值（带有长度前缀）追加到 rep_
}

// 向 WriteBatch 中添加一个 Delete 操作
void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1); // 操作计数加 1
  rep_.push_back(static_cast<char>(kTypeDeletion)); // 添加 Delete 操作的类型标记
  PutLengthPrefixedSlice(&rep_, key);               // 将键（带有长度前缀）追加到 rep_
}

// 将另一个 WriteBatch (source) 的内容追加到当前 WriteBatch
void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source); // 调用内部辅助函数实现追加
}

namespace { // 匿名命名空间，限制以下类的作用域在此文件内
// MemTableInserter 类实现了 WriteBatch::Handler 接口，用于将 WriteBatch 中的操作插入到 MemTable
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_; // 操作的起始序列号
  MemTable* mem_;           // 目标 MemTable

  // 处理 Put 操作：将键值对添加到 MemTable
  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value); // 调用 MemTable::Add 方法
    sequence_++; // 序列号递增
  }
  // 处理 Delete 操作：将删除标记添加到 MemTable
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice()); // 调用 MemTable::Add 方法，值为一个空的 Slice
    sequence_++; // 序列号递增
  }
};
}  // namespace

// 将 WriteBatch (b) 中的所有操作插入到指定的 MemTable (memtable) 中
Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter; // 创建 MemTableInserter 实例
  inserter.sequence_ = WriteBatchInternal::Sequence(b); // 设置起始序列号
  inserter.mem_ = memtable;                             // 设置目标 MemTable
  return b->Iterate(&inserter); // 迭代 WriteBatch 并通过 inserter 处理每个操作
}

// 用给定的内容 (contents) 设置 WriteBatch (b) 的内部表示 (rep_)
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader); // 断言内容大小至少包含头部
  b->rep_.assign(contents.data(), contents.size()); // 将 contents 的数据赋给 b->rep_
}

// 将源 WriteBatch (src) 的数据部分追加到目标 WriteBatch (dst)
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src)); // 更新目标 WriteBatch 的操作计数
  assert(src->rep_.size() >= kHeader);    // 断言源 WriteBatch 大小至少包含头部
  // 将源 WriteBatch 的数据部分 (跳过头部) 追加到目标 WriteBatch 的 rep_
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb