// Copyright (c) 2017 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_EXPORT_H_ // 防止头文件被重复包含的宏保护
#define STORAGE_LEVELDB_INCLUDE_EXPORT_H_

// LEVELDB_EXPORT 宏用于控制符号（如类和函数）在动态链接库（DLL/.so）中的可见性。
// 它确保当 LevelDB 被编译为共享库时，必要的符号被正确导出（供库外部使用）或导入（当使用库时）。

#if !defined(LEVELDB_EXPORT) // 检查 LEVELDB_EXPORT 是否尚未被定义

#if defined(LEVELDB_SHARED_LIBRARY) // 检查是否正在构建或使用 LevelDB 的共享库版本
// --- 开始处理共享库的情况 ---

#if defined(_WIN32) // 检查是否为 Windows 平台 (MSVC 编译器通常定义 _WIN32)
// --- 开始处理 Windows 平台 ---

#if defined(LEVELDB_COMPILE_LIBRARY) // 检查是否正在编译 LevelDB 库本身
// 当编译 LevelDB 共享库（DLL）时，将符号标记为导出
#define LEVELDB_EXPORT __declspec(dllexport)
#else // 当使用（链接到）LevelDB 共享库（DLL）时
// 将符号标记为从 DLL 导入
#define LEVELDB_EXPORT __declspec(dllimport)
#endif  // defined(LEVELDB_COMPILE_LIBRARY)

// --- 结束处理 Windows 平台 ---
#else  // 非 Windows 平台 (例如 Linux, macOS, 使用 GCC/Clang 等编译器)
// --- 开始处理非 Windows 平台 ---

#if defined(LEVELDB_COMPILE_LIBRARY) // 检查是否正在编译 LevelDB 库本身
// 当编译 LevelDB 共享库（.so）时，将符号标记为默认可见性（使其可导出）
// GCC/Clang 使用 __attribute__((visibility("default"))) 来导出符号
#define LEVELDB_EXPORT __attribute__((visibility("default")))
#else // 当使用（链接到）LevelDB 共享库（.so）时
// 对于 GCC/Clang，导入符号通常不需要特殊标记，所以定义为空
#define LEVELDB_EXPORT
#endif

// --- 结束处理非 Windows 平台 ---
#endif  // defined(_WIN32)

// --- 结束处理共享库的情况 ---
#else  // 如果不是共享库 (例如，静态库或直接包含源码)
// --- 开始处理非共享库的情况 ---

// 对于静态库或直接编译源码，不需要特殊的导出/导入标记，所以定义为空
#define LEVELDB_EXPORT

// --- 结束处理非共享库的情况 ---
#endif // defined(LEVELDB_SHARED_LIBRARY)

#endif  // !defined(LEVELDB_EXPORT)

#endif  // STORAGE_LEVELDB_INCLUDE_EXPORT_H_