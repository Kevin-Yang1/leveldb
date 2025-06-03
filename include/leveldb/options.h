// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
#define STORAGE_LEVELDB_INCLUDE_OPTIONS_H_

#include <cstddef> // 引入 C 标准定义，例如 size_t

#include "leveldb/export.h" // 引入导出宏定义

namespace leveldb {

// 前向声明
class Cache;
class Comparator;
class Env;
class FilterPolicy;
class Logger;
class Snapshot;

// DB 内容存储在一系列块中，每个块包含一系列键值对。
// 每个块在存储到文件之前可能会被压缩。
// 以下枚举描述了用于压缩块的压缩方法（如果有）。
enum CompressionType {
  // 注意：不要更改现有条目的值，因为这些是磁盘上持久格式的一部分。
  kNoCompression = 0x0,     // 不压缩
  kSnappyCompression = 0x1, // 使用 Snappy 压缩
  kZstdCompression = 0x2,   // 使用 Zstd 压缩
};

// 控制数据库行为的选项 (传递给 DB::Open)
struct LEVELDB_EXPORT Options {
  // 创建一个 Options 对象，所有字段都使用默认值。
  Options();

  // -------------------
  // 影响行为的参数

  // 用于定义表中键顺序的比较器。
  // 默认值：一个使用字典序字节顺序的比较器。
  //
  // 要求：客户端必须确保此处提供的比较器与先前在同一数据库上调用 open 时
  // 提供的比较器具有相同的名称并且以完全相同的方式对键进行排序。
  const Comparator* comparator;

  // 如果为 true，则在数据库丢失时创建它。
  bool create_if_missing = false;

  // 如果为 true，则在数据库已存在时引发错误。
  bool error_if_exists = false;

  // 如果为 true，实现将对其正在处理的数据进行积极检查，
  // 如果检测到任何错误，将提前停止。这可能会产生不可预见的后果：
  // 例如，一个 DB 条目的损坏可能导致大量条目变得不可读，
  // 或者整个 DB 变得无法打开。
  bool paranoid_checks = false;

  // 使用指定的对象与环境交互，
  // 例如读/写文件、调度后台工作等。
  // 默认值：Env::Default()
  Env* env;

  // 数据库生成的任何内部进度/错误信息都将写入 info_log (如果非空)，
  // 或者写入与 DB 内容存储在同一目录中的文件 (如果 info_log 为空)。
  Logger* info_log = nullptr;

  // -------------------
  // 影响性能的参数

  // 在转换为已排序的磁盘文件之前，在内存中构建的数据量（由磁盘上的未排序日志支持）。
  //
  // 较大的值可以提高性能，尤其是在批量加载期间。
  // 最多可以同时在内存中保留两个写缓冲区，
  // 因此您可能希望调整此参数以控制内存使用。
  // 此外，较大的写缓冲区将导致下次打开数据库时恢复时间更长。
  size_t write_buffer_size = 4 * 1024 * 1024; // 写缓冲区大小，默认为 4MB

  // DB 可以使用的打开文件数。如果您的数据库具有较大的工作集
  // (每个工作集的 2MB 预算一个打开文件)，则可能需要增加此值。
  int max_open_files = 1000; // 最大打开文件数，默认为 1000

  // 对块的控制 (用户数据存储在一系列块中，块是从磁盘读取的单位)。

  // 如果非空，则使用指定的缓存来存储块。
  // 如果为空，leveldb 将自动创建并使用一个 8MB 的内部缓存。
  Cache* block_cache = nullptr; // 块缓存

  // 每个块中打包的用户数据的大致大小。请注意，此处指定的块大小
  // 对应于未压缩的数据。如果启用了压缩，则从磁盘读取的单元的
  // 实际大小可能会更小。此参数可以动态更改。
  size_t block_size = 4 * 1024; // 块大小，默认为 4KB

  // 用于键的增量编码的重启点之间的键数。
  // 此参数可以动态更改。大多数客户端应保持此参数不变。
  int block_restart_interval = 16; // 块重启间隔，默认为 16

  // Leveldb 在切换到新文件之前最多会向一个文件写入这么多字节。
  // 大多数客户端应保持此参数不变。但是，如果您的文件系统
  // 对较大的文件更有效，则可以考虑增加该值。缺点是
  // Compaction 时间更长，因此延迟/性能瓶颈更长。
  // 增加此参数的另一个原因可能是当您最初填充大型数据库时。
  size_t max_file_size = 2 * 1024 * 1024; // 最大文件大小，默认为 2MB

  // 使用指定的压缩算法压缩块。此参数可以动态更改。
  //
  // 默认值：kSnappyCompression，它提供轻量级但快速的压缩。
  //
  // kSnappyCompression 在 Intel(R) Core(TM)2 2.4GHz 上的典型速度：
  //    ~200-500MB/s 压缩速度
  //    ~400-800MB/s 解压缩速度
  // 请注意，这些速度明显快于大多数持久存储速度，
  // 因此通常不值得切换到 kNoCompression。即使输入数据
  // 不可压缩，kSnappyCompression 实现也会有效地检测到这一点，
  // 并切换到未压缩模式。
  CompressionType compression = kSnappyCompression; // 压缩类型，默认为 Snappy

  // zstd 的压缩级别。
  // 当前仅支持 [-5,22] 范围。默认值为 1。
  int zstd_compression_level = 1; // Zstd 压缩级别，默认为 1

  // 实验性：如果为 true，则在打开数据库时附加到现有的 MANIFEST 和日志文件。
  // 这可以显著加快打开速度。
  //
  // 默认值：当前为 false，但以后可能会变为 true。
  bool reuse_logs = false; // 是否重用日志文件

  // 如果非空，则使用指定的过滤器策略来减少磁盘读取。
  // 许多应用程序将受益于在此处传递 NewBloomFilterPolicy() 的结果。
  const FilterPolicy* filter_policy = nullptr; // 过滤器策略
};

// 控制读操作的选项
struct LEVELDB_EXPORT ReadOptions {
  // 如果为 true，则从底层存储读取的所有数据都将根据相应的校验和进行验证。
  bool verify_checksums = false; // 是否校验校验和

  // 为此迭代读取的数据是否应缓存在内存中？
  // 调用者可能希望对批量扫描将此字段设置为 false。
  bool fill_cache = true; // 是否填充缓存

  // 如果 "snapshot" 非空，则按提供的快照读取
  // (该快照必须属于正在读取的数据库，并且不得已释放)。
  // 如果 "snapshot" 为空，则使用此读取操作开始时状态的隐式快照。
  const Snapshot* snapshot = nullptr; // 快照
};

// 控制写操作的选项
struct LEVELDB_EXPORT WriteOptions {
  WriteOptions() = default; // 默认构造函数

  // 如果为 true，则在写入被视为完成之前，将从操作系统
  // 缓冲区缓存中刷新写入 (通过调用 WritableFile::Sync())。
  // 如果此标志为 true，则写入速度会变慢。
  //
  // 如果此标志为 false，并且机器崩溃，则某些最近的写入可能会丢失。
  // 请注意，如果只是进程崩溃 (即机器没有重新启动)，
  // 即使 sync==false，也不会丢失任何写入。
  //
  // 换句话说，sync==false 的 DB 写入具有与 "write()" 系统调用类似的
  // 崩溃语义。sync==true 的 DB 写入具有与 "write()" 系统调用
  // 后跟 "fsync()" 类似的崩溃语义。
  bool sync = false; // 是否同步写入
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_OPTIONS_H_