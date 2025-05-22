// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>   // 用于原子操作，如 std::atomic<size_t>
#include <cassert>  // 用于断言，如 assert()
#include <cstddef>  // 定义了像 size_t 这样的类型
#include <cstdint>  // 定义了像 uintptr_t 这样的整型
#include <vector>   // 用于 std::vector，存储已分配的内存块指针

namespace leveldb {

// Arena 类用于高效地分配一系列小块内存。
// 它通过从预先分配的大块内存中划分出小块来工作，
// 从而避免了频繁调用 malloc/new 的开销。
// 当 Arena 对象被销毁时，所有通过它分配的内存都会被一次性释放。
// Arena 不是线程安全的，除非另有说明。
class Arena {
 public:
  // 构造函数
  Arena();

  // 禁用拷贝构造函数和拷贝赋值运算符，防止 Arena 对象被意外拷贝。
  // 拷贝 Arena 可能会导致内存管理混乱（例如，重复释放或内存泄漏）。
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  // 析构函数，释放所有已分配的内存块。
  ~Arena();

  // 返回一个指向新分配的 "bytes" 字节内存块的指针。
  // 这个方法尝试从当前内存块中快速分配。如果当前块空间不足，
  // 它会调用 AllocateFallback 来分配新的内存块。
  // 返回的内存不保证特定的对齐方式，除了 malloc 通常提供的对齐。
  char* Allocate(size_t bytes);

  // 分配具有 malloc 提供的正常对齐保证的内存。
  // 这通常意味着返回的地址适合任何基本数据类型。
  // 如果需要更严格的对齐（例如，SIMD 操作），则需要自定义实现。
  char* AllocateAligned(size_t bytes);

  // 返回 Arena 已分配数据总内存使用量的估计值。
  // 这个值包括所有已分配的内存块的大小以及存储这些块指针的开销。
  size_t MemoryUsage() const {
    // 使用 memory_order_relaxed 是因为我们不需要严格的顺序保证，
    // 只需要一个原子读取来获取近似的内存使用量。
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  // 当 Allocate 方法发现当前内存块空间不足时，调用此方法。
  // 它会决定是分配一个刚好满足需求的大块，还是分配一个新的标准大小的块。
  char* AllocateFallback(size_t bytes);
  // 分配一个新的内存块，大小为 block_bytes，并将其添加到 blocks_ 列表中。
  char* AllocateNewBlock(size_t block_bytes);

  // 分配状态
  char* alloc_ptr_;                 // 指向当前内存块中下一个可分配字节的指针
  size_t alloc_bytes_remaining_;    // 当前内存块中剩余的可分配字节数

  // 存储所有通过 new[] 分配的内存块的指针。
  // Arena 析构时会遍历这个 vector 并释放所有内存块。
  std::vector<char*> blocks_;

  // Arena 的总内存使用量。
  // 使用 std::atomic 是为了在多线程环境中（如果 Arena 被这样使用的话，尽管它本身不是线程安全的）
  // 或者在某些特殊情况下提供更安全的内存使用量读取。
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  // 这个 TODO 指出 memory_usage_ 是原子访问的，但其他成员（如 alloc_ptr_）不是。
  // 这暗示了 Arena 的设计可能主要针对单线程使用，或者 memory_usage_ 的原子性
  // 是为了在特定场景下（如从不同线程监控内存）提供一种相对安全的读取方式，
  // 而不是为了使整个 Arena 线程安全。
  std::atomic<size_t> memory_usage_;
};

// Allocate 方法的内联实现，用于快速路径分配。
inline char* Arena::Allocate(size_t bytes) {
  // 如果允许分配0字节，返回值的语义会有点混乱，
  // 所以这里禁止了（LevelDB 内部使用不需要0字节分配）。
  assert(bytes > 0); // 断言确保请求的字节数大于0。
  // 如果当前内存块有足够的剩余空间
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;          // 将当前分配指针作为结果
    alloc_ptr_ += bytes;                // 向后移动分配指针
    alloc_bytes_remaining_ -= bytes;    // 减少剩余字节数
    return result;                      // 返回分配的内存地址
  }
  // 如果当前块空间不足，则调用后备分配方法。
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_