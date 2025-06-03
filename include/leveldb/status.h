// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
// Status 封装了操作的结果。它可能表示成功，也可能表示带有相关错误消息的错误。
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.
// 多个线程可以在没有外部同步的情况下调用 Status 上的 const 方法，
// 但是如果任何线程可能调用非 const 方法，则所有访问相同 Status 的线程都必须使用外部同步。

#ifndef STORAGE_LEVELDB_INCLUDE_STATUS_H_
#define STORAGE_LEVELDB_INCLUDE_STATUS_H_

#include <algorithm> // 引入 C++ 标准库算法，例如 std::swap
#include <string>    // 引入 C++ 标准库字符串

#include "leveldb/export.h" // 引入导出宏定义
#include "leveldb/slice.h"  // 引入 Slice 类定义

namespace leveldb {

// Status 类用于表示操作的返回状态
class LEVELDB_EXPORT Status {
 public:
  // 创建一个表示成功的 Status 对象。
  Status() noexcept : state_(nullptr) {} // 默认构造函数，state_ 为空指针表示成功
  ~Status() { delete[] state_; } // 析构函数，释放 state_ 指向的内存

  // 拷贝构造函数
  Status(const Status& rhs);
  // 拷贝赋值运算符
  Status& operator=(const Status& rhs);

  // 移动构造函数
  Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
  // 移动赋值运算符
  Status& operator=(Status&& rhs) noexcept;

  // 返回一个表示成功的 Status 对象。
  static Status OK() { return Status(); }

  // 返回表示特定类型错误的 Status 对象。
  // msg: 主要错误信息
  // msg2: 次要或附加错误信息 (可选)
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }

  // 如果状态表示成功，则返回 true。
  bool ok() const { return (state_ == nullptr); }

  // 如果状态表示未找到 (NotFound) 错误，则返回 true。
  bool IsNotFound() const { return code() == kNotFound; }

  // 如果状态表示数据损坏 (Corruption) 错误，则返回 true。
  bool IsCorruption() const { return code() == kCorruption; }

  // 如果状态表示 IO 错误，则返回 true。
  bool IsIOError() const { return code() == kIOError; }

  // 如果状态表示不支持的操作 (NotSupported) 错误，则返回 true。
  bool IsNotSupportedError() const { return code() == kNotSupported; }

  // 如果状态表示无效参数 (InvalidArgument) 错误，则返回 true。
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // 返回此状态的字符串表示形式，适合打印。
  // 对于成功状态，返回字符串 "OK"。
  std::string ToString() const;

 private:
  // 状态码枚举
  enum Code {
    kOk = 0,             // 成功
    kNotFound = 1,       // 未找到
    kCorruption = 2,     // 数据损坏
    kNotSupported = 3,   // 不支持的操作
    kInvalidArgument = 4, // 无效参数
    kIOError = 5         // IO 错误
  };

  // 获取状态码
  Code code() const {
    // 如果 state_ 为空指针，表示成功 (kOk)，否则从 state_ 的第5个字节获取状态码
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  // 内部构造函数，用于创建带有错误码和消息的 Status 对象
  Status(Code code, const Slice& msg, const Slice& msg2);
  // 复制状态字符串的辅助函数，返回一个新的字符数组指针
  static const char* CopyState(const char* s);

  // OK 状态 (成功状态) 的 state_ 为空指针。
  // 否则，state_ 是一个 new[] 分配的字符数组，其格式如下：
  //    state_[0..3] == 消息的长度 (4字节整数)
  //    state_[4]    == 状态码 (1字节)
  //    state_[5..]  == 错误消息本身 (字符序列)
  const char* state_;
};

// Status 的拷贝构造函数的内联实现
inline Status::Status(const Status& rhs) {
  // 如果 rhs.state_ 为空指针，则将当前对象的 state_ 也设置为空指针，
  // 否则，调用 CopyState 复制 rhs.state_ 的内容。
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

// Status 的拷贝赋值运算符的内联实现
inline Status& Status::operator=(const Status& rhs) {
  // 下面的条件同时处理了自赋值 (this == &rhs) 的情况，
  // 以及 rhs 和 *this 都是成功状态 (state_ 都为 nullptr) 的常见情况。
  if (state_ != rhs.state_) {
    delete[] state_; // 先释放当前对象可能持有的 state_ 内存
    // 如果 rhs.state_ 为空指针，则将当前对象的 state_ 也设置为空指针，
    // 否则，调用 CopyState 复制 rhs.state_ 的内容。
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this; // 返回当前对象的引用
}

// Status 的移动赋值运算符的内联实现
inline Status& Status::operator=(Status&& rhs) noexcept {
  // 使用 std::swap 高效地交换当前对象和 rhs 对象的 state_ 指针，
  // 这样 rhs 对象在之后析构时会释放其原来的 state_ (如果它之前指向了 *this 的 state_)，
  // 或者如果 rhs.state_ 原本就是 nullptr，则什么也不做。
  // 当前对象则接管了 rhs 原来的 state_。
  std::swap(state_, rhs.state_);
  return *this; // 返回当前对象的引用
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATUS_H_