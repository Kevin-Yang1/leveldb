// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

// 从 data 指针处解析一个长度前缀的 Slice。
// data 的格式应该是：varint32 编码的长度，后跟实际数据。
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  // 解析 varint32 编码的长度，p 会移动到实际数据部分的开头。
  // p + 5 是为了防止读取越界，假设 varint32 最多占用5字节。
  p = GetVarint32Ptr(p, p + 5, &len);
  return Slice(p, len); // 返回包含实际数据的 Slice
}

// MemTable 构造函数
// 初始化比较器、引用计数和底层的跳表。
MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

// MemTable 析构函数
// 断言在析构时引用计数必须为0，确保对象是通过 Unref() 正确释放的。
MemTable::~MemTable() { assert(refs_ == 0); }

// 返回 MemTable 大致占用的内存大小，即 Arena 分配的内存量。
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

// MemTable 内部使用的 KeyComparator 的比较函数。
// 跳表中的键是编码后的内部键（包含长度前缀）。
int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // 内部键被编码为长度前缀的字符串。
  // 首先从原始指针中提取出实际的键 Slice。
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  // 使用传入的 InternalKeyComparator 比较这两个内部键。
  return comparator.Compare(a, b);
}

// 将目标 Slice 编码成适合在跳表中查找的内部键格式（长度前缀），并返回其指针。
// 使用传入的 scratch 字符串作为临时存储空间，返回的指针指向 scratch 内部。
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear(); // 清空临时字符串
  PutVarint32(scratch, target.size()); // 将 target 的长度以 varint32 格式追加到 scratch
  scratch->append(target.data(), target.size()); // 将 target 的数据追加到 scratch
  return scratch->data(); // 返回 scratch 中数据的指针
}

// MemTableIterator 类，用于遍历 MemTable 中的内容。
// 它封装了 SkipList 的迭代器。
class MemTableIterator : public Iterator {
 public:
  // 构造函数，接收一个指向 MemTable 内部跳表的指针。
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete; // 禁止拷贝构造
  MemTableIterator& operator=(const MemTableIterator&) = delete; // 禁止拷贝赋值

  ~MemTableIterator() override = default; // 默认析构函数

  // 检查迭代器是否指向一个有效的位置。
  bool Valid() const override { return iter_.Valid(); }
  // 定位到第一个键大于或等于 k 的条目。
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  // 定位到第一个条目。
  void SeekToFirst() override { iter_.SeekToFirst(); }
  // 定位到最后一个条目。
  void SeekToLast() override { iter_.SeekToLast(); }
  // 移动到下一个条目。
  void Next() override { iter_.Next(); }
  // 移动到上一个条目。
  void Prev() override { iter_.Prev(); }
  // 返回当前条目的键 (Slice)。键是从跳表节点中解析出来的。
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  // 返回当前条目的值 (Slice)。值也是从跳表节点中解析出来的，位于键之后。
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    // 值的长度前缀紧跟在键数据之后。
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  // 返回迭代器的状态，对于 MemTableIterator 总是 OK。
  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_; // 底层的跳表迭代器
  std::string tmp_;  // 用于 Seek 时 EncodeKey 的临时存储空间
};

// 创建并返回一个新的 MemTableIterator。
Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

// 向 MemTable 中添加一个条目。
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // MemTable 中条目的存储格式如下：
  //  key_size     : internal_key.size() 的 varint32 编码
  //  key bytes    : char[internal_key.size()] (实际的内部键数据)
  //  tag          : uint64((sequence << 8) | type) (序列号和类型)
  //  value_size   : value.size() 的 varint32 编码
  //  value bytes  : char[value.size()] (实际的值数据)
  //
  // 注意：这里的 "key bytes" 指的是 InternalKey，它由 user_key 和 tag (sequence + type) 组成。
  // 但在 MemTable 的物理存储中，tag 是显式跟在 user_key 之后的，而不是作为 InternalKey 的一部分预先编码。
  // 所以，"key_size" 实际上是 user_key.size()。
  // "internal_key_size" 变量名在这里指的是 user_key.size() + 8 (tag 的大小)。

  size_t key_size = key.size(); // 用户键的实际大小
  size_t val_size = value.size(); // 值的实际大小
  size_t internal_key_size = key_size + 8; // 内部键的大小 = 用户键大小 + 8字节的tag (序列号和类型)
  // 计算整个条目编码后的总长度
  const size_t encoded_len = VarintLength(internal_key_size) + // 内部键长度的 varint32 编码所需字节数
                             internal_key_size +             // 内部键本身 (user_key + tag) 的字节数
                             VarintLength(val_size) +        // 值长度的 varint32 编码所需字节数
                             val_size;                       // 值本身的字节数
  char* buf = arena_.Allocate(encoded_len); // 从 Arena 分配存储整个条目的内存
  char* p = EncodeVarint32(buf, internal_key_size); // 首先编码内部键的长度
  std::memcpy(p, key.data(), key_size); // 拷贝用户键数据
  p += key_size; // 移动指针到用户键数据之后
  EncodeFixed64(p, (s << 8) | type); // 编码 8 字节的 tag (序列号和类型)
  p += 8; // 移动指针到 tag 之后
  p = EncodeVarint32(p, val_size); // 编码值的长度
  std::memcpy(p, value.data(), val_size); // 拷贝值数据
  assert(p + val_size == buf + encoded_len); // 断言指针最终位置正确，确保所有数据都被正确写入
  table_.Insert(buf); // 将指向编码后条目的指针插入到跳表中，跳表会根据 KeyComparator 进行排序
                      // 跳表中的键实际上是 buf 指针，比较时会通过 GetLengthPrefixedSlice 解析出内部键进行比较
}

// 从 MemTable 中获取与 LookupKey 对应的条目。
bool MemTable::Get(const LookupKey& lkey, std::string* value, Status* s) {
  Slice memkey = lkey.memtable_key(); // 获取用于在 MemTable 中查找的键 (klength + userkey + tag)
  Table::Iterator iter(&table_); // 创建跳表迭代器
  iter.Seek(memkey.data()); // 在跳表中查找第一个键大于或等于 memkey 的条目
                            // memkey.data() 指向的是编码后的内部键，跳表比较器会处理它
  if (iter.Valid()) { // 如果找到了一个有效的条目
    // 条目格式:
    //    klength  varint32 (内部键的长度)
    //    userkey  char[user_key_length]
    //    tag      uint64
    //    vlength  varint32 (值的长度)
    //    value    char[vlength]
    const char* entry = iter.key(); // 获取指向跳表中存储的完整条目数据的指针
    uint32_t key_length; // 用于存储解析出来的内部键的长度
    // 解析内部键的长度，key_ptr 指向用户键的开始
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    // 比较查找到的条目的用户键是否与目标用户键相同。
    // key_length - 8 是因为 key_length 是内部键的长度 (user_key + tag)，所以用户键长度是 key_length - 8。
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), lkey.user_key()) == 0) {
      // 用户键匹配成功
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); // 解码 tag
      switch (static_cast<ValueType>(tag & 0xff)) { // 获取类型 (tag 的最低字节)
        case kTypeValue: { // 如果是普通值类型
          // 解析值的 Slice (值的长度前缀在 tag 之后，即 key_ptr + key_length 处)
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size()); // 将值赋给输出参数
          return true;
        }
        case kTypeDeletion: // 如果是删除标记类型
          *s = Status::NotFound(Slice()); // 设置状态为 NotFound
          return true; // 即使是删除标记，也表示找到了该键的记录
      }
    }
  }
  return false; // 没有找到匹配的条目
}

}  // namespace leveldb