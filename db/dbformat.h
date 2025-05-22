// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_ // 防止头文件重复包含宏定义
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef> // 定义 size_t 等类型
#include <cstdint> // 定义 uint64_t, uint8_t 等整型
#include <string>  // 引入 std::string

#include "leveldb/comparator.h" // 引入比较器接口
#include "leveldb/db.h"         // 引入 LevelDB 的公共 API 定义
#include "leveldb/filter_policy.h" // 引入布隆过滤器策略接口
#include "leveldb/slice.h"      // 引入 Slice 数据结构
#include "leveldb/table_builder.h" // 引入 Table 构建器接口
#include "util/coding.h"      // 引入编码/解码工具函数
#include "util/logging.h"     // 引入日志记录工具 (主要用于断言)

namespace leveldb { // 定义 leveldb 命名空间

// 常量配置的分组。其中一些参数未来可能通过 Options 设置。
namespace config {
static const int kNumLevels = 7; // LSM-Tree 的总层数 (Level-0 到 Level-6)

// 当 Level-0 的文件数量达到此阈值时，触发 Level-0 的合并操作。
static const int kL0_CompactionTrigger = 4;

// Level-0 文件数量的软限制。达到此阈值时，会减慢写入速度。
static const int kL0_SlowdownWritesTrigger = 8;

// Level-0 文件数量的最大限制。达到此阈值时，会停止写入操作。
static const int kL0_StopWritesTrigger = 12;

// 如果新的（从 memtable dump 下来的）SSTable 不会造成重叠，
// 它会被推向的最大层级。我们尝试推到 Level-2，以避免相对昂贵的
// Level-0 => Level-1 合并，并避免一些昂贵的 MANIFEST 文件操作。
// 我们不一直推到最大层级，因为如果相同的键空间被反复覆盖，
// 可能会产生大量浪费的磁盘空间。
static const int kMaxMemCompactLevel = 2;

// 迭代过程中读取数据的采样间隔（字节）。
static const int kReadBytesPeriod = 1048576; // 1MB

}  // namespace config

class InternalKey; // 前向声明 InternalKey 类

// 值类型，编码为内部键的最后一个组成部分。
// 注意：不要更改这些枚举值，它们已嵌入到磁盘上的数据结构中。
enum ValueType {
  kTypeDeletion = 0x0, // 表示删除操作的标记
  kTypeValue = 0x1     // 表示普通值类型的标记
};
// kValueTypeForSeek 定义了在为特定序列号构建 ParsedInternalKey 对象进行查找时应传递的 ValueType。
// （因为我们按序列号降序排序，并且值类型作为内部键中序列号的低8位嵌入，
// 所以我们需要使用编号最大的 ValueType，而不是最小的）。
static const ValueType kValueTypeForSeek = kTypeValue; // 查找时使用的值类型

typedef uint64_t SequenceNumber; // 定义序列号的类型为 uint64_t

// 我们在底部保留了八位空位，以便类型和序列号可以打包到64位中。
// 因此序列号最大占用 56 位。
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1); // 最大序列号

// 解析后的内部键结构体
struct ParsedInternalKey {
  Slice user_key;       // 用户键
  SequenceNumber sequence; // 序列号
  ValueType type;          // 值类型 (kTypeDeletion 或 kTypeValue)

  ParsedInternalKey() {}  // 故意不初始化成员 (为了速度)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {} // 构造函数
  std::string DebugString() const; // 返回用于调试的字符串表示
};

// 返回 "key" 编码后的长度。
// 内部键的编码长度 = 用户键长度 + 8字节 (序列号和类型)。
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// 将 "key" 的序列化形式追加到 *result 中。
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// 尝试从 "internal_key" 解析内部键。成功时，将解析的数据存储在 "*result" 中，并返回 true。
// 失败时返回 false，"*result" 的状态未定义。
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// 返回内部键的用户键部分。
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8); // 内部键至少包含8字节的序列号和类型
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// 内部键的比较器。它使用指定的用户键比较器来比较用户键部分，
// 并通过递减的序列号来打破平局（即序列号大的排在前面）。
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_; // 指向用户提供的比较器

 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  const char* Name() const override; // 返回比较器的名称
  int Compare(const Slice& a, const Slice& b) const override; // 比较两个序列化的内部键
  // 找到一个最短的分隔符，使得 start <= separator < limit
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override;
  // 找到一个比 key 稍大的最短后继键
  void FindShortSuccessor(std::string* key) const override;

  const Comparator* user_comparator() const { return user_comparator_; } // 获取用户比较器

  int Compare(const InternalKey& a, const InternalKey& b) const; // 比较两个 InternalKey 对象
};

// 过滤器策略的包装器，将内部键转换为用户键。
// 这样布隆过滤器等策略可以直接在用户键上操作。
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_; // 指向用户提供的过滤器策略

 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override; // 返回过滤器策略的名称
  // 根据一组键创建过滤器
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  // 检查键是否可能匹配过滤器
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};

// 此目录中的模块应将内部键包装在此类中，而不是使用普通字符串，
// 以避免错误地使用字符串比较代替 InternalKeyComparator。
class InternalKey {
 private:
  std::string rep_; // 内部键的字符串表示 (user_key + sequence_number + type)

 public:
  InternalKey() {}  // 将 rep_ 留空以表示无效
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    // 通过 ParsedInternalKey 构造并追加到 rep_
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  // 从 Slice s 解码内部键到 rep_。
  bool DecodeFrom(const Slice& s) {
    rep_.assign(s.data(), s.size());
    return !rep_.empty(); // 如果 rep_ 非空则解码成功
  }

  // 返回内部键的编码后 Slice 表示。
  Slice Encode() const {
    assert(!rep_.empty()); // 必须是有效的内部键
    return rep_;
  }

  // 返回内部键的用户键部分。
  Slice user_key() const { return ExtractUserKey(rep_); }

  // 从 ParsedInternalKey p 设置内部键。
  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  // 清除内部键，使其无效。
  void Clear() { rep_.clear(); }

  std::string DebugString() const; // 返回用于调试的字符串表示
};

// 内联实现 InternalKeyComparator::Compare(const InternalKey&, const InternalKey&)
inline int InternalKeyComparator::Compare(const InternalKey& a,
                                          const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode()); // 调用比较 Slice 的版本
}

// 内联实现 ParseInternalKey
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false; // 内部键长度至少为8字节
  // 从 internal_key 的末尾8字节解码出 序列号和类型 (tag)
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint8_t c = num & 0xff; // 低8位是类型
  result->sequence = num >> 8; // 高56位是序列号
  result->type = static_cast<ValueType>(c); // 转换为 ValueType 枚举
  result->user_key = Slice(internal_key.data(), n - 8); // 前面的部分是用户键
  return (c <= static_cast<uint8_t>(kTypeValue)); // 检查类型是否有效
}

// 一个辅助类，用于 DBImpl::Get() 操作，封装了查找时所需的各种键格式。
class LookupKey {
 public:
  // 为在具有指定序列号的快照中查找 user_key 来初始化 *this。
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  LookupKey(const LookupKey&) = delete; // 禁止拷贝构造
  LookupKey& operator=(const LookupKey&) = delete; // 禁止拷贝赋值

  ~LookupKey(); // 析构函数，用于释放在堆上分配的内存

  // 返回适用于在 MemTable 中查找的键。
  // 格式: varint32(user_key.size() + 8) | user_key | sequence_number (8 bytes) | type (in sequence_number)
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // 返回一个内部键 (适用于传递给内部迭代器)。
  // 格式: user_key | sequence_number (8 bytes) | type (in sequence_number)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // 返回用户键。
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // 我们构造一个如下形式的字符数组：
  //    klength  varint32               <-- start_ 指向这里 (整个 memtable key 的开始)
  //    userkey  char[klength]          <-- kstart_ 指向这里 (internal key 的开始, 也是 user_key 的开始)
  //    tag      uint64 (sequence + type)
  //                                    <-- end_ 指向整个编码后数据的末尾的下一个字节
  // 这个数组是一个合适的 MemTable 键。
  // 以 "userkey" 开头的后缀可以用作 InternalKey。
  const char* start_;  // 指向 memtable_key 的起始位置
  const char* kstart_; // 指向 internal_key (即 user_key) 的起始位置
  const char* end_;    // 指向整个编码数据的末尾之后
  char space_[200];  // 为短键避免动态内存分配，提供一个栈上的缓冲区
};

// LookupKey 的析构函数实现
inline LookupKey::~LookupKey() {
  if (start_ != space_) { // 如果 start_ 指向的不是栈上的 space_ 数组，说明内存是在堆上分配的
    delete[] start_;      // 释放堆内存
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_