// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>   // 用于 snprintf 等 C 标准IO函数 (虽然在此文件中未直接使用)
#include <sstream>  // 用于 std::ostringstream，实现字符串流操作

#include "port/port.h"      // 平台相关的可移植性封装 (例如线程、原子操作等)
#include "util/coding.h"    // 包含各种编码/解码函数 (如 Fixed64, Varint32)

namespace leveldb {

// 将序列号和值类型打包成一个 uint64_t。
// 序列号占用高56位，值类型占用低8位。
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber); // 断言序列号未超过最大值
  assert(t <= kValueTypeForSeek);    // 断言值类型有效 (kTypeDeletion 或 kTypeValue)
  return (seq << 8) | t; // 将序列号左移8位，然后与类型进行或运算
}

// 将解析后的内部键 (ParsedInternalKey) 编码并追加到字符串 result 中。
// 编码格式为：user_key + (sequence_number << 8 | type)。
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size()); // 追加用户键
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type)); // 追加打包后的序列号和类型
}

// 返回 ParsedInternalKey 的调试字符串表示。
// 格式为：'user_key' @ sequence_number : type
std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss; // 创建字符串输出流
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type); // 格式化输出
  return ss.str(); // 返回字符串结果
}

// 返回 InternalKey 的调试字符串表示。
// 如果内部键有效，则解析并返回 ParsedInternalKey 的调试字符串。
// 如果无效，则返回 "(bad)" 加上转义后的原始字符串。
std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) { // 尝试解析内部键
    return parsed.DebugString(); // 解析成功，返回解析后的调试字符串
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_); // 解析失败，标记为 "(bad)"
  return ss.str();
}

// 返回 InternalKeyComparator 的名称。
const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator"; // 比较器名称
}

// 比较两个序列化的内部键 akey 和 bkey。
// 比较规则：
// 1. 按用户键升序 (根据用户提供的比较器)。
// 2. 如果用户键相同，则按序列号降序 (序列号大的排前面)。
// 3. 如果序列号也相同（理论上不应发生，因为序列号用于区分版本），则按类型降序 (类型值大的排前面)。
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // 首先比较用户键部分
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) { // 如果用户键相同
    // 提取并比较序列号和类型 (打包在最后8字节)
    // 序列号大的排前面 (降序)，所以比较结果要反转
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) { // a 的序列号/类型更大 (意味着 a 更“新”)
      r = -1; // a 排在 b 前面
    } else if (anum < bnum) {
      r = +1; // b 排在 a 前面
    }
    // 如果 anum == bnum，则 r 保持为 0，表示完全相等
  }
  return r;
}

// 找到一个最短的分隔符 (separator)，使得 start <= separator < limit。
// 这个函数用于 SSTable 的索引块，以减少索引条目的大小。
void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // 尝试缩短键的用户部分
  Slice user_start = ExtractUserKey(*start); // 提取 start 的用户键
  Slice user_limit = ExtractUserKey(limit);  // 提取 limit 的用户键
  std::string tmp(user_start.data(), user_start.size()); // 复制 user_start 到 tmp
  user_comparator_->FindShortestSeparator(&tmp, user_limit); // 调用用户比较器的 FindShortestSeparator
  // 如果缩短后的用户键 tmp 物理上变短了，并且逻辑上大于原始的 user_start
  // (即 user_comparator_->Compare(user_start, tmp) < 0)
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // 用户键物理上变短，但逻辑上变大了。
    // 将最早可能的序列号和类型 (kMaxSequenceNumber, kValueTypeForSeek) 附加到缩短的用户键上。
    // kValueTypeForSeek 是 kTypeValue，确保了它是最大的类型值。
    // kMaxSequenceNumber 确保了它是最大的序列号。
    // 这样构造出的内部键是大于等于原始 start 且小于 limit 的最短键。
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0); // 断言新键大于原始 start
    assert(this->Compare(tmp, limit) < 0);  // 断言新键小于 limit
    start->swap(tmp); // 用缩短后的键替换原来的 start
  }
}

// 找到一个比 key 稍大的最短后继键 (successor)。
// 用于 SSTable 的索引块。
void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key); // 提取用户键
  std::string tmp(user_key.data(), user_key.size()); // 复制用户键到 tmp
  user_comparator_->FindShortSuccessor(&tmp); // 调用用户比较器的 FindShortSuccessor
  // 如果后继用户键 tmp 物理上变短了，并且逻辑上大于原始的 user_key
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // 用户键物理上变短，但逻辑上变大了。
    // 附加最早可能的序列号和类型。
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0); // 断言新键大于原始 key
    key->swap(tmp); // 用后继键替换原始 key
  }
}

// 返回 InternalFilterPolicy 的名称，实际上是其包装的用户过滤器策略的名称。
const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

// 根据一组内部键创建过滤器。
// 它会先提取所有键的用户键部分，然后调用用户提供的过滤器策略来创建过滤器。
void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // 我们依赖于 table.cc 中的代码不介意我们调整 keys[] 的事实。
  // （这里将 const Slice* 转换为 Slice* 是为了修改 Slice 的内容，这通常是不安全的，
  //  但注释表明这是基于对调用者行为的特定假设。）
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]); // 将内部键替换为其用户键部分
    // TODO(sanjay): 是否需要抑制重复的用户键？
  }
  user_policy_->CreateFilter(keys, n, dst); // 使用用户键创建过滤器
}

// 检查一个内部键是否可能匹配给定的过滤器。
// 它会提取键的用户键部分，然后调用用户提供的过滤器策略进行检查。
bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f); // 使用用户键进行匹配检查
}

// LookupKey 构造函数。
// 为在具有指定序列号 s 的快照中查找 user_key 来初始化 LookupKey 对象。
// LookupKey 用于封装 Get 操作时所需的各种键格式。
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size(); // 用户键的长度
  // 估算需要的总空间：用户键长度 + 内部键长度的 varint32 编码 (最多5字节) + 8字节的tag
  // 13 = 8 (tag) + 5 (varint32 for internal_key_length)
  size_t needed = usize + 13;
  char* dst; // 指向目标缓冲区的指针
  if (needed <= sizeof(space_)) { // 如果需要的空间小于预分配的栈空间 space_
    dst = space_; // 使用栈上的 space_ 数组
  } else { // 否则，在堆上分配内存
    dst = new char[needed];
  }
  start_ = dst; // start_ 指向缓冲区的开始，即 memtable_key 的开始
  // 编码内部键的长度 (user_key.size() + 8) 到 dst，并更新 dst 指针
  // 这个长度是 memtable 中存储的键的第一个变长整数
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst; // kstart_ 指向用户键的开始，即 internal_key 的开始
  std::memcpy(dst, user_key.data(), usize); // 拷贝用户键数据
  dst += usize; // 移动 dst 指针到用户键之后
  // 编码序列号和类型 (kValueTypeForSeek 即 kTypeValue) 到 dst
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8; // 移动 dst 指针到 tag 之后
  end_ = dst; // end_ 指向整个编码数据的末尾的下一个字节
}

}  // namespace leveldb