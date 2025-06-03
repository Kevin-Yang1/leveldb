// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_PORT_PORT_STDCXX_H_ // 防止头文件重复包含
#define STORAGE_LEVELDB_PORT_PORT_STDCXX_H_

// port/port_config.h 的可用性通过 __has_include 在较新的编译器中自动检测。
// 如果定义了 LEVELDB_HAS_PORT_CONFIG_H，它将覆盖配置检测。
#if defined(LEVELDB_HAS_PORT_CONFIG_H) // 检查是否定义了 LEVELDB_HAS_PORT_CONFIG_H

#if LEVELDB_HAS_PORT_CONFIG_H
#include "port/port_config.h" // 如果 LEVELDB_HAS_PORT_CONFIG_H 为真，则包含 port_config.h
#endif  // LEVELDB_HAS_PORT_CONFIG_H

#elif defined(__has_include) // 否则，检查编译器是否支持 __has_include

#if __has_include("port/port_config.h") // 如果支持且能找到 port_config.h
#include "port/port_config.h" // 包含 port_config.h
#endif  // __has_include("port/port_config.h")

#endif  // defined(LEVELDB_HAS_PORT_CONFIG_H)

#if HAVE_CRC32C // 如果配置了 CRC32C 支持
#include <crc32c/crc32c.h> // 包含 CRC32C 库头文件
#endif  // HAVE_CRC32C
#if HAVE_SNAPPY // 如果配置了 Snappy 支持
#include <snappy.h> // 包含 Snappy 库头文件
#endif  // HAVE_SNAPPY
#if HAVE_ZSTD // 如果配置了 ZSTD 支持
#define ZSTD_STATIC_LINKING_ONLY  // 用于 ZSTD_compressionParameters，定义静态链接
#include <zstd.h> // 包含 ZSTD 库头文件
#endif  // HAVE_ZSTD

#include <cassert> // 引入断言
#include <condition_variable>  // NOLINT，引入条件变量
#include <cstddef> // 引入 size_t 等定义
#include <cstdint> // 引入固定宽度整数类型
#include <mutex>  // NOLINT，引入互斥锁
#include <string> // 引入字符串

#include "port/thread_annotations.h" // 引入线程注解

namespace leveldb {
namespace port {

class CondVar; // 前向声明条件变量类

// 对 std::mutex 的简单封装
class LOCKABLE Mutex { // LOCKABLE 是线程注解，表示这是一个可锁对象
 public:
  Mutex() = default; // 默认构造函数
  ~Mutex() = default; // 默认析构函数

  Mutex(const Mutex&) = delete; // 禁止拷贝构造
  Mutex& operator=(const Mutex&) = delete; // 禁止拷贝赋值

  void Lock() EXCLUSIVE_LOCK_FUNCTION() { mu_.lock(); } // 加锁，EXCLUSIVE_LOCK_FUNCTION 是线程注解，标记此函数获取锁
  void Unlock() UNLOCK_FUNCTION() { mu_.unlock(); } // 解锁，UNLOCK_FUNCTION 是线程注解，标记此函数释放锁
  void AssertHeld() ASSERT_EXCLUSIVE_LOCK() {} // 断言当前线程持有锁，ASSERT_EXCLUSIVE_LOCK 是线程注解

 private:
  friend class CondVar; // CondVar 类是友元，可以访问其私有成员
  std::mutex mu_; // 底层的 std::mutex 对象
};

// 对 std::condition_variable 的简单封装
class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) { assert(mu != nullptr); } // 构造函数，需要一个 Mutex 指针
  ~CondVar() = default; // 默认析构函数

  CondVar(const CondVar&) = delete; // 禁止拷贝构造
  CondVar& operator=(const CondVar&) = delete; // 禁止拷贝赋值

  void Wait() { // 等待条件满足
    std::unique_lock<std::mutex> lock(mu_->mu_, std::adopt_lock); // 使用 std::adopt_lock 表示当前已持有锁
    cv_.wait(lock); // 等待条件变量，会自动释放锁并在唤醒后重新获取锁
    lock.release(); // 释放 unique_lock 对锁的所有权，因为锁的生命周期由外部 Mutex 管理
  }
  void Signal() { cv_.notify_one(); } // 唤醒一个等待的线程
  void SignalAll() { cv_.notify_all(); } // 唤醒所有等待的线程

 private:
  std::condition_variable cv_; // 底层的 std::condition_variable 对象
  Mutex* const mu_; // 指向关联的 Mutex 对象
};

// 使用 Snappy 压缩数据
inline bool Snappy_Compress(const char* input, size_t length,
                            std::string* output) {
#if HAVE_SNAPPY // 如果 Snappy 可用
  output->resize(snappy::MaxCompressedLength(length)); // 调整输出字符串大小为最大压缩长度
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen); // 执行原始压缩
  output->resize(outlen); // 调整输出字符串大小为实际压缩长度
  return true;
#else
  // 消除编译器关于未使用参数的警告
  (void)input;
  (void)length;
  (void)output;
#endif  // HAVE_SNAPPY

  return false; // 如果 Snappy 不可用，返回 false
}

// 获取 Snappy 压缩数据的未压缩长度
inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#if HAVE_SNAPPY // 如果 Snappy 可用
  return snappy::GetUncompressedLength(input, length, result); // 获取未压缩长度
#else
  // 消除编译器关于未使用参数的警告
  (void)input;
  (void)length;
  (void)result;
  return false; // 如果 Snappy 不可用，返回 false
#endif  // HAVE_SNAPPY
}

// 使用 Snappy 解压缩数据
inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_SNAPPY // 如果 Snappy 可用
  return snappy::RawUncompress(input, length, output); // 执行原始解压缩
#else
  // 消除编译器关于未使用参数的警告
  (void)input;
  (void)length;
  (void)output;
  return false; // 如果 Snappy 不可用，返回 false
#endif  // HAVE_SNAPPY
}

// 使用 Zstd 压缩数据
inline bool Zstd_Compress(int level, const char* input, size_t length,
                          std::string* output) {
#if HAVE_ZSTD // 如果 Zstd 可用
  // 获取最大压缩长度
  size_t outlen = ZSTD_compressBound(length);
  if (ZSTD_isError(outlen)) { // 检查是否有错误
    return false;
  }
  output->resize(outlen); // 调整输出字符串大小
  ZSTD_CCtx* ctx = ZSTD_createCCtx(); // 创建压缩上下文
  ZSTD_compressionParameters parameters =
      ZSTD_getCParams(level, std::max(length, size_t{1}), /*dictSize=*/0); // 获取压缩参数
  ZSTD_CCtx_setCParams(ctx, parameters); // 设置压缩参数
  outlen = ZSTD_compress2(ctx, &(*output)[0], output->size(), input, length); // 执行压缩
  ZSTD_freeCCtx(ctx); // 释放压缩上下文
  if (ZSTD_isError(outlen)) { // 检查是否有错误
    return false;
  }
  output->resize(outlen); // 调整输出字符串大小为实际压缩长度
  return true;
#else
  // 消除编译器关于未使用参数的警告
  (void)level;
  (void)input;
  (void)length;
  (void)output;
  return false; // 如果 Zstd 不可用，返回 false
#endif  // HAVE_ZSTD
}

// 获取 Zstd 压缩数据的未压缩长度
inline bool Zstd_GetUncompressedLength(const char* input, size_t length,
                                       size_t* result) {
#if HAVE_ZSTD // 如果 Zstd 可用
  size_t size = ZSTD_getFrameContentSize(input, length); // 获取帧内容大小
  if (size == 0) return false; // 如果大小为0，可能表示错误或空数据
  *result = size;
  return true;
#else
  // 消除编译器关于未使用参数的警告
  (void)input;
  (void)length;
  (void)result;
  return false; // 如果 Zstd 不可用，返回 false
#endif  // HAVE_ZSTD
}

// 使用 Zstd 解压缩数据
inline bool Zstd_Uncompress(const char* input, size_t length, char* output) {
#if HAVE_ZSTD // 如果 Zstd 可用
  size_t outlen;
  if (!Zstd_GetUncompressedLength(input, length, &outlen)) { // 先获取未压缩长度
    return false;
  }
  ZSTD_DCtx* ctx = ZSTD_createDCtx(); // 创建解压缩上下文
  outlen = ZSTD_decompressDCtx(ctx, output, outlen, input, length); // 执行解压缩
  ZSTD_freeDCtx(ctx); // 释放解压缩上下文
  if (ZSTD_isError(outlen)) { // 检查是否有错误
    return false;
  }
  return true;
#else
  // 消除编译器关于未使用参数的警告
  (void)input;
  (void)length;
  (void)output;
  return false; // 如果 Zstd 不可用，返回 false
#endif  // HAVE_ZSTD
}

// 获取堆分析信息 (通常用于调试和性能分析，此处为空实现)
inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  // 消除编译器关于未使用参数的警告
  (void)func;
  (void)arg;
  return false; // 默认返回 false，表示不支持或未实现
}

// 计算 CRC32C 校验和 (硬件加速版本)
inline uint32_t AcceleratedCRC32C(uint32_t crc, const char* buf, size_t size) {
#if HAVE_CRC32C // 如果 CRC32C (特别是硬件加速) 可用
  return ::crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(buf), size); // 使用 crc32c 库计算
#else
  // 消除编译器关于未使用参数的警告
  (void)crc;
  (void)buf;
  (void)size;
  return 0; // 如果不可用，返回 0 (或一个固定的值，具体取决于错误处理策略)
#endif  // HAVE_CRC32C
}

}  // namespace port
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_STDCXX_H_