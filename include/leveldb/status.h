// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Status 类封装了一个操作的结果。它可能表示成功，
// 或者表示一个带有相关错误信息的错误。
//
// 多个线程可以无需外部同步地调用 Status 对象的 const 方法，
// 但是如果任何线程可能调用非 const 方法，则所有访问同一 Status 对象的线程
// 都必须使用外部同步。

#ifndef STORAGE_LEVELDB_INCLUDE_STATUS_H_
#define STORAGE_LEVELDB_INCLUDE_STATUS_H_

#include <algorithm> // 包含算法相关的头文件，例如 std::swap
#include <string>    // 包含字符串相关的头文件

#include "leveldb/export.h" // 引入用于控制符号可见性的宏
#include "leveldb/slice.h"  // 引入 Slice 类，用于表示字符串片段

namespace leveldb {

class LEVELDB_EXPORT Status {
 public:
  // 创建一个表示成功的 Status 对象。
  // noexcept 表明此构造函数不会抛出异常。
  Status() noexcept : state_(nullptr) {}
  // 析构函数，释放 state_ 指向的内存。
  ~Status() { delete[] state_; }

  // 拷贝构造函数。
  Status(const Status& rhs);
  // 拷贝赋值运算符。
  Status& operator=(const Status& rhs);

  // 移动构造函数。
  // noexcept 表明此构造函数不会抛出异常。
  Status(Status&& rhs) noexcept : state_(rhs.state_) {
    // 将源对象的 state_ 置空，防止其析构函数释放内存。
    rhs.state_ = nullptr;
  }
  // 移动赋值运算符。
  // noexcept 表明此运算符不会抛出异常。
  Status& operator=(Status&& rhs) noexcept;

  // 返回一个表示成功的 Status 对象。
  static Status OK() { return Status(); }

  // 返回表示特定类型错误的 Status 对象。
  // msg: 主要错误信息。
  // msg2: 可选的附加错误信息。
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

  // 如果状态表示 NotFound 错误，则返回 true。
  bool IsNotFound() const { return code() == kNotFound; }

  // 如果状态表示 Corruption 错误，则返回 true。
  bool IsCorruption() const { return code() == kCorruption; }

  // 如果状态表示 IOError 错误，则返回 true。
  bool IsIOError() const { return code() == kIOError; }

  // 如果状态表示 NotSupported 错误，则返回 true。
  bool IsNotSupportedError() const { return code() == kNotSupported; }

  // 如果状态表示 InvalidArgument 错误，则返回 true。
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // 返回此状态的字符串表示形式，适合打印。
  // 对于成功状态，返回字符串 "OK"。
  std::string ToString() const;

 private:
  // 错误码枚举。
  enum Code {
    kOk = 0,            // 成功
    kNotFound = 1,      // 未找到
    kCorruption = 2,    // 数据损坏
    kNotSupported = 3,  // 不支持的操作
    kInvalidArgument = 4, // 无效参数
    kIOError = 5        // 输入/输出错误
  };

  // 返回当前状态的错误码。
  // 如果 state_ 为空（表示成功），则返回 kOk。
  // 否则，从 state_ 数组的第4个字节获取错误码。
  Code code() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  // 私有构造函数，用于创建带有错误码和消息的 Status 对象。
  Status(Code code, const Slice& msg, const Slice& msg2);
  // 辅助函数，用于复制状态字符串。
  static const char* CopyState(const char* s);

  // OK 状态的 state_ 为 nullptr。
  // 否则，state_ 是一个 new[] 分配的字符数组，其格式如下：
  //    state_[0..3] == 消息的长度 (4字节，小端存储)
  //    state_[4]    == 错误码 (1字节)
  //    state_[5..]  == 错误消息本身
  const char* state_;
};

// 内联拷贝构造函数的实现。
inline Status::Status(const Status& rhs) {
  // 如果源对象的 state_ 为空，则当前对象的 state_ 也为空。
  // 否则，复制源对象的 state_。
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

// 内联拷贝赋值运算符的实现。
inline Status& Status::operator=(const Status& rhs) {
  // 以下条件捕获了自赋值 (this == &rhs) 的情况，
  // 以及 rhs 和 *this 都为 OK 状态的常见情况。
  if (state_ != rhs.state_) {
    delete[] state_; // 先释放当前对象的 state_
    // 如果源对象的 state_ 为空，则当前对象的 state_ 也为空。
    // 否则，复制源对象的 state_。
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this;
}

// 内联移动赋值运算符的实现。
inline Status& Status::operator=(Status&& rhs) noexcept {
  // 交换当前对象和源对象的 state_ 指针。
  // 源对象的 state_ 将变为 nullptr（如果当前对象之前是 OK 状态），
  // 或者指向当前对象之前的状态数据。
  // 当前对象将接管源对象的状态数据。
  std::swap(state_, rhs.state_);
  return *this;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATUS_H_