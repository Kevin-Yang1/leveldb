// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

// Arena 分配内存块的默认大小 (4KB)
static const int kBlockSize = 4096;

// Arena 构造函数
// 初始化时，当前分配指针为空，剩余字节为0，总内存使用量为0。
Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

// Arena 析构函数
// 释放所有已分配的内存块。
Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i]; // 释放每个内存块
  }
}

// 当当前内存块空间不足时，调用此方法来分配内存。
// 这是一个后备分配策略。
char* Arena::AllocateFallback(size_t bytes) {
  // 如果请求的字节数大于 kBlockSize 的 1/4，
  // 则单独为这个对象分配一个大小刚好的新块，
  // 以避免在剩余字节中浪费过多空间。
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // 否则，分配一个新的标准大小 (kBlockSize) 的内存块。
  // 当前块中剩余的空间将被浪费掉。
  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize); // 分配新块
  alloc_bytes_remaining_ = kBlockSize;      // 更新剩余字节数

  char* result = alloc_ptr_;      // 从新块的起始位置开始分配
  alloc_ptr_ += bytes;            // 移动分配指针
  alloc_bytes_remaining_ -= bytes; // 更新剩余字节数
  return result;
}

// 分配对齐的内存。
// 内存对齐到指针大小（通常是4或8字节），但至少是8字节。
char* Arena::AllocateAligned(size_t bytes) {
  // 计算对齐要求。在64位系统上通常是8字节，32位系统上是4字节，但这里确保至少是8字节。
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 编译时断言，确保对齐值是2的幂。eg: 如果 align = 8 (0b1000), align - 1 = 7 (0b0111)
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  // 计算当前分配指针 (alloc_ptr_) 相对于对齐值的偏移量。
  // (align - 1) 是一个掩码，例如 align=8 时，掩码是 0b111。
  // current_mod 是 alloc_ptr_ 地址的低几位，表示其对齐余数。
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  // 计算需要填充多少字节 (slop) 才能达到对齐边界。
  // 如果 current_mod 为 0，则 alloc_ptr_ 已经对齐，slop 为 0。
  // 否则，slop = align - current_mod。
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // 总共需要的字节数 = 请求的字节数 + 填充字节数。
  size_t needed = bytes + slop;
  char* result;
  // 如果当前块剩余空间足够容纳 needed 字节
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop; // 对齐后的起始地址
    alloc_ptr_ += needed;       // 移动分配指针
    alloc_bytes_remaining_ -= needed; // 更新剩余字节数
  } else {
    // 当前块空间不足，调用 AllocateFallback 来获取内存。
    // AllocateFallback 总是返回对齐的内存（因为它要么分配一个新块，要么返回一个单独分配的块，
    // 而 new char[] 通常会返回至少满足基本数据类型对齐要求的内存）。
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  // 断言确保返回的地址确实是按 align 对齐的。
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

// 分配一个新的内存块，并将其添加到 blocks_ 列表中。
// 更新总内存使用量。
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes]; // 使用 new 分配原始内存
  blocks_.push_back(result);            // 将新块的指针存入列表
  // 更新内存使用量，原子地增加 block_bytes (块本身大小)
  // 和 sizeof(char*) (存储块指针在 blocks_ vector 中占用的近似空间)。
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb