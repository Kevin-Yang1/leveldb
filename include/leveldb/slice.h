// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice 是一个简单的数据结构，包含一个指向外部存储区域的指针和一个大小。
// Slice 的使用者必须确保在相应的外部存储被释放后不再使用该 Slice。
//
// 多个线程可以无需外部同步地调用 Slice 对象的 const 方法，
// 但是如果任何线程可能调用非 const 方法，则所有访问同一 Slice 对象的线程
// 都必须使用外部同步。

#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <cassert> // 用于断言，进行运行时检查
#include <cstddef> // 定义了像 size_t 这样的类型
#include <cstring> // 包含 C 字符串处理函数，如 strlen, memcmp
#include <string>  // 包含 std::string 类

#include "leveldb/export.h" // 引入用于控制符号可见性的宏

namespace leveldb {

// Slice 类，用于表示一个连续的字符序列（通常是字节序列）。
// 它不拥有底层数据，只是指向数据的指针和数据长度。
class LEVELDB_EXPORT Slice {
 public:
  // 创建一个空的 Slice。
  Slice() : data_(""), size_(0) {}

  // 创建一个引用 d[0,n-1] 的 Slice。
  // d: 指向字符数组的指针。
  // n: 字符数组的长度。
  Slice(const char* d, size_t n) : data_(d), size_(n) {}

  // 创建一个引用 std::string "s" 内容的 Slice。
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // 创建一个引用 C 风格字符串 s[0,strlen(s)-1] 的 Slice。
  Slice(const char* s) : data_(s), size_(strlen(s)) {}

  // 故意设计为可拷贝的。
  // 使用默认的拷贝构造函数。
  Slice(const Slice&) = default;
  // 使用默认的拷贝赋值运算符。
  Slice& operator=(const Slice&) = default;

  // 返回指向被引用数据起始位置的指针。
  const char* data() const { return data_; }

  // 返回被引用数据的长度（以字节为单位）。
  size_t size() const { return size_; }

  // 如果被引用数据的长度为零，则返回 true。
  bool empty() const { return size_ == 0; }

  // 返回被引用数据中的第 n 个字节。
  // 要求：n < size()
  char operator[](size_t n) const {
    assert(n < size()); // 断言索引 n 在有效范围内
    return data_[n];
  }

  // 将此 Slice 更改为空数组的引用。
  void clear() {
    data_ = ""; // 指向空字符串
    size_ = 0;   // 大小设为0
  }

  // 从此 Slice 中移除前 "n" 个字节。
  // 要求：n <= size()
  void remove_prefix(size_t n) {
    assert(n <= size()); // 断言 n 不超过当前大小
    data_ += n;          // 指针向后移动 n 个字节
    size_ -= n;          // 大小减去 n
  }

  // 返回一个包含被引用数据副本的 std::string。
  std::string ToString() const { return std::string(data_, size_); }

  // 三路比较。返回值：
  //   < 0  如果 "*this" <  "b",
  //   == 0 如果 "*this" == "b",
  //   > 0  如果 "*this" >  "b"
  int compare(const Slice& b) const;

  // 如果 "x" 是 "*this" 的前缀，则返回 true。
  bool starts_with(const Slice& x) const {
    // 检查当前 Slice 的长度是否大于等于 x 的长度，
    // 并且两个 Slice 的前 x.size_ 个字节是否相同。
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  const char* data_; // 指向数据的指针，常量指针，数据不可修改，指针可以修改（data_ += n;）
  size_t size_;      // 数据的大小
};

// 重载等于运算符 (==)，用于比较两个 Slice 是否相等。
inline bool operator==(const Slice& x, const Slice& y) {
  // 两个 Slice 相等，当且仅当它们的长度相同，并且对应的数据内容也相同。
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

// 重载不等于运算符 (!=)，用于比较两个 Slice 是否不相等。
inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

// Slice 类的 compare 方法的内联实现。
inline int Slice::compare(const Slice& b) const {
  // 确定两个 Slice 中较短的那个的长度。
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  // 比较两个 Slice 在 min_len 长度内的字节。
  int r = memcmp(data_, b.data_, min_len);
  // 如果在 min_len 长度内它们是相等的，则根据长度来决定大小关系。
  if (r == 0) {
    if (size_ < b.size_)
      r = -1; // 当前 Slice 较短，所以更小
    else if (size_ > b.size_)
      r = +1; // 当前 Slice 较长，所以更大
  }
  return r;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_