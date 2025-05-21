
leveldb
=======

_Jeff Dean, Sanjay Ghemawat_

leveldb 库提供了一个持久化的键值存储。键和值是任意字节数组。键值存储中的键根据用户指定的比较器函数进行排序。

## 打开数据库

leveldb 数据库有一个名称，该名称对应于一个文件系统目录。数据库的所有内容都存储在此目录中。以下示例显示了如何打开数据库，如果数据库不存在则创建它：

```c++
#include <cassert>
#include "leveldb/db.h"

leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
assert(status.ok());
...
````

如果你希望在数据库已存在时引发错误，请在 `leveldb::DB::Open` 调用之前添加以下行：

```c++
options.error_if_exists = true;
```

## 状态 (Status)

你可能已经注意到上面出现的 `leveldb::Status` 类型。leveldb 中大多数可能遇到错误的函数都会返回此类型的值。你可以检查这样的结果是否正常，并打印相关的错误消息：

```c++
leveldb::Status s = ...;
if (!s.ok()) cerr << s.ToString() << endl;
```

## 关闭数据库

当你使用完数据库后，只需删除数据库对象即可。示例：

```c++
... 按上述方式打开数据库 ...
... 对数据库进行一些操作 ...
delete db;
```

## 读取和写入

数据库提供 `Put`、`Delete` 和 `Get` 方法来修改/查询数据库。例如，以下代码将存储在 `key1` 下的值移动到 `key2`。

```c++
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);
```

## 原子更新

请注意，如果进程在 `Put key2` 之后但在 `delete key1` 之前终止，则相同的值可能保留在多个键下。可以通过使用 `WriteBatch` 类以原子方式应用一组更新来避免此类问题：

```c++
#include "leveldb/write_batch.h"
...
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) {
  leveldb::WriteBatch batch;
  batch.Delete(key1);
  batch.Put(key2, value);
  s = db->Write(leveldb::WriteOptions(), &batch);
}
```

`WriteBatch` 保存要对数据库进行的一系列编辑，并且批处理中的这些编辑按顺序应用。请注意，我们在 `Put` 之前调用了 `Delete`，这样如果 `key1` 与 `key2` 相同，我们就不会最终错误地完全删除该值。

除了原子性方面的好处外，`WriteBatch` 还可以通过将大量单独的修改放入同一批处理中来加速批量更新。

## 同步写入

默认情况下，对 leveldb 的每次写入都是异步的：它在将写入从进程推送到操作系统后返回。从操作系统内存到基础持久存储的传输是异步发生的。可以为特定写入打开同步标志，以使写入操作在正在写入的数据已完全推送到持久存储之前不会返回。（在 Posix 系统上，这是通过在写入操作返回之前调用 `fsync(...)` 或 `fdatasync(...)` 或 `msync(..., MS_SYNC)` 来实现的。）

```c++
leveldb::WriteOptions write_options;
write_options.sync = true;
db->Put(write_options, ...);
```

异步写入通常比同步写入快一千倍以上。异步写入的缺点是机器崩溃可能会导致最后几次更新丢失。请注意，仅写入进程崩溃（即不是重新启动）不会导致任何损失，因为即使 `sync` 为 false，更新也会在被视为完成之前从进程内存推送到操作系统。

异步写入通常可以安全使用。例如，在将大量数据加载到数据库中时，你可以通过在崩溃后重新启动批量加载来处理丢失的更新。混合方案也是可能的，其中每 N 次写入是同步的，并且在发生崩溃时，批量加载会在上一次运行完成的最后一次同步写入之后重新启动。（同步写入可以更新一个标记，描述在崩溃时从何处重新启动。）

`WriteBatch` 提供了异步写入的替代方案。可以将多个更新放入同一个 `WriteBatch` 中，并使用同步写入（即 `write_options.sync` 设置为 true）一起应用。同步写入的额外成本将分摊到批处理中的所有写入中。

## 并发

一个数据库一次只能由一个进程打开。leveldb 实现会从操作系统获取一个锁以防止滥用。在单个进程中，同一个 `leveldb::DB` 对象可以由多个并发线程安全地共享。也就是说，不同的线程可以在同一个数据库上写入、获取迭代器或调用 `Get`，而无需任何外部同步（leveldb 实现会自动进行所需的同步）。但是，其他对象（如 `Iterator` 和 `WriteBatch`）可能需要外部同步。如果两个线程共享这样一个对象，它们必须使用自己的锁定协议来保护对它的访问。更多详细信息可在公共头文件中找到。

## 迭代

以下示例演示了如何打印数据库中的所有键值对。

```c++
leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
}
assert(it->status().ok());  // 检查扫描过程中发现的任何错误
delete it;
```

以下变体显示了如何仅处理范围 `[start,limit)` 中的键：

```c++
for (it->Seek(start);
    it->Valid() && it->key().ToString() < limit;
    it->Next()) {
  ...
}
```

你还可以按相反的顺序处理条目。（注意：反向迭代可能比正向迭代稍慢。）

```c++
for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ...
}
```

## 快照 (Snapshots)

快照提供了对键值存储整个状态的一致的只读视图。`ReadOptions::snapshot` 可以为非 NULL，以指示读取应在数据库状态的特定版本上操作。如果 `ReadOptions::snapshot` 为 NULL，则读取将在当前状态的隐式快照上操作。

快照由 `DB::GetSnapshot()` 方法创建：

```c++
leveldb::ReadOptions options;
options.snapshot = db->GetSnapshot();
... 对数据库应用一些更新 ...
leveldb::Iterator* iter = db->NewIterator(options);
... 使用 iter 读取以查看创建快照时的状态 ...
delete iter;
db->ReleaseSnapshot(options.snapshot);
```

请注意，当不再需要快照时，应使用 `DB::ReleaseSnapshot` 接口将其释放。这允许实现摆脱仅为支持在该快照时间点读取而维护的状态。

## Slice

上面 `it->key()` 和 `it->value()` 调用的返回值是 `leveldb::Slice` 类型的实例。Slice 是一个简单的结构，包含一个长度和一个指向外部字节数组的指针。返回 Slice 比返回 `std::string` 更经济，因为我们不需要复制可能很大的键和值。此外，leveldb 方法不返回以空字符结尾的 C 风格字符串，因为 leveldb 键和值允许包含 `'\0'` 字节。

C++ 字符串和以空字符结尾的 C 风格字符串可以轻松转换为 Slice：

```c++
leveldb::Slice s1 = "hello";

std::string str("world");
leveldb::Slice s2 = str;
```

Slice 可以轻松转换回 C++ 字符串：

```c++
std::string str = s1.ToString();
assert(str == std::string("hello"));
```

使用 Slice 时要小心，因为调用者需要确保 Slice 指向的外部字节数组在 Slice 使用期间保持活动状态。例如，以下代码是错误的：

```c++
leveldb::Slice slice;
if (...) {
  std::string str = ...;
  slice = str;
}
Use(slice);
```

当 if 语句超出作用域时，`str` 将被销毁，并且 `slice` 的后备存储将消失。

## 比较器 (Comparators)

前面的示例使用了键的默认排序函数，该函数按字典顺序对字节进行排序。但是，你可以在打开数据库时提供自定义比较器。例如，假设每个数据库键由两个数字组成，我们应该按第一个数字排序，并按第二个数字打破平局。首先，定义一个 `leveldb::Comparator` 的适当子类来表达这些规则：

```c++
class TwoPartComparator : public leveldb::Comparator {
 public:
  // 三向比较函数：
  //   如果 a < b：负结果
  //   如果 a > b：正结果
  //   否则：零结果
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    int a1, a2, b1, b2;
    ParseKey(a, &a1, &a2);
    ParseKey(b, &b1, &b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }

  //暂时忽略以下方法：
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
  void FindShortSuccessor(std::string*) const {}
};
```

现在使用此自定义比较器创建一个数据库：

```c++
TwoPartComparator cmp;
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
options.comparator = &cmp;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
...
```

\#\#\#向后兼容性

比较器的 `Name` 方法的结果在创建数据库时附加到数据库，并在每次后续打开数据库时进行检查。如果名称更改，`leveldb::DB::Open` 调用将失败。因此，仅当新的键格式和比较函数与现有数据库不兼容，并且可以丢弃所有现有数据库的内容时，才更改名称。

但是，通过一些预先计划，你仍然可以随着时间的推移逐步改进键格式。例如，你可以在每个键的末尾存储一个版本号（对于大多数用途，一个字节就足够了）。当你希望切换到新的键格式时（例如，向 `TwoPartComparator` 处理的键添加可选的第三部分），(a) 保持相同的比较器名称 (b) 增加新键的版本号 (c) 更改比较器函数，使其使用键中找到的版本号来决定如何解释它们。

## 性能

可以通过更改 `include/options.h` 中定义的类型的默认值来调整性能。

### 块大小 (Block size)

leveldb 将相邻的键组合到同一个块中，这样的块是与持久存储进行传输的单元。默认块大小约为 4096 未压缩字节。主要对数据库内容进行批量扫描的应用程序可能希望增加此大小。对小值进行大量点读取的应用程序，如果性能测量表明有所改进，则可能希望切换到较小的块大小。使用小于一千字节或大于几兆字节的块没有太多好处。另请注意，对于较大的块大小，压缩将更有效。

### 压缩 (Compression)

每个块在写入持久存储之前都会单独压缩。默认情况下启用压缩，因为默认压缩方法非常快，并且会自动禁用不可压缩的数据。在极少数情况下，应用程序可能希望完全禁用压缩，但仅当基准测试显示性能有所提高时才应这样做：

```c++
leveldb::Options options;
options.compression = leveldb::kNoCompression;
... leveldb::DB::Open(options, name, ...) ....
```

### 缓存 (Cache)

数据库的内容存储在文件系统中的一组文件中，每个文件存储一系列压缩块。如果 `options.block_cache` 不为 NULL，则它用于缓存常用的未压缩块内容。

```c++
#include "leveldb/cache.h"

leveldb::Options options;
options.block_cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB 缓存
leveldb::DB* db;
leveldb::DB::Open(options, name, &db);
... 使用数据库 ...
delete db
delete options.block_cache;
```

请注意，缓存保存未压缩的数据，因此应根据应用程序级别的数据大小来确定其大小，而不考虑压缩带来的任何减少。（压缩块的缓存留给操作系统缓冲缓存或客户端提供的任何自定义 `Env` 实现。）

执行批量读取时，应用程序可能希望禁用缓存，以便批量读取处理的数据最终不会取代大部分缓存内容。可以使用每个迭代器的选项来实现此目的：

```c++
leveldb::ReadOptions options;
options.fill_cache = false;
leveldb::Iterator* it = db->NewIterator(options);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ...
}
delete it;
```

### 键布局 (Key Layout)

请注意，磁盘传输和缓存的单位是块。相邻的键（根据数据库排序顺序）通常会放在同一个块中。因此，应用程序可以通过将一起访问的键放在彼此靠近的位置，并将不常用的键放在键空间的一个单独区域中来提高其性能。

例如，假设我们正在 leveldb 之上实现一个简单的文件系统。我们可能希望存储的条目类型是：

```
filename -> permission-bits, length, list of file_block_ids
file_block_id -> data
```

我们可能希望用一个字母（例如“/”）作为文件名键的前缀，用另一个字母（例如“0”）作为 `file_block_id` 键的前缀，这样仅对元数据进行扫描就不会强制我们获取和缓存庞大的文件内容。

### 过滤器 (Filters)

由于 leveldb 数据在磁盘上的组织方式，单个 `Get()` 调用可能涉及多次磁盘读取。可选的 `FilterPolicy` 机制可用于大幅减少磁盘读取次数。

```c++
leveldb::Options options;
options.filter_policy = NewBloomFilterPolicy(10);
leveldb::DB* db;
leveldb::DB::Open(options, "/tmp/testdb", &db);
... 使用数据库 ...
delete db;
delete options.filter_policy;
```

前面的代码将基于布隆过滤器的过滤策略与数据库相关联。基于布隆过滤器的过滤依赖于在内存中为每个键保留一定数量的位数据（在本例中为每个键 10 位，因为这是我们传递给 `NewBloomFilterPolicy` 的参数）。此过滤器将使 `Get()` 调用所需的不必要的磁盘读取次数减少约 100 倍。增加每个键的位数将导致更大的减少，但代价是更多的内存使用。我们建议工作集不适合内存且进行大量随机读取的应用程序设置过滤器策略。

如果你使用的是自定义比较器，则应确保你使用的过滤器策略与你的比较器兼容。例如，考虑一个在比较键时忽略尾随空格的比较器。`NewBloomFilterPolicy` 不得与此类比较器一起使用。相反，应用程序应提供一个也忽略尾随空格的自定义过滤器策略。例如：

```c++
class CustomFilterPolicy : public leveldb::FilterPolicy {
 private:
  leveldb::FilterPolicy* builtin_policy_;

 public:
  CustomFilterPolicy() : builtin_policy_(leveldb::NewBloomFilterPolicy(10)) {}
  ~CustomFilterPolicy() { delete builtin_policy_; }

  const char* Name() const { return "IgnoreTrailingSpacesFilter"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const {
    // 删除尾随空格后使用内置布隆过滤器代码
    std::vector<leveldb::Slice> trimmed(n);
    for (int i = 0; i < n; i++) {
      trimmed[i] = RemoveTrailingSpaces(keys[i]);
    }
    builtin_policy_->CreateFilter(trimmed.data(), n, dst);
  }
};
```

高级应用程序可以提供不使用布隆过滤器但使用其他机制来汇总一组键的过滤器策略。有关详细信息，请参阅 `leveldb/filter_policy.h`。

## 校验和 (Checksums)

leveldb 将校验和与其存储在文件系统中的所有数据相关联。提供了两个单独的控件来控制这些校验和的验证程度：

可以将 `ReadOptions::verify_checksums` 设置为 true，以强制对代表特定读取从文件系统读取的所有数据进行校验和验证。默认情况下，不执行此类验证。

可以在打开数据库之前将 `Options::paranoid_checks` 设置为 true，以使数据库实现在检测到内部损坏后立即引发错误。根据数据库的哪个部分已损坏，错误可能在打开数据库时引发，也可能稍后由另一个数据库操作引发。默认情况下，偏执检查是关闭的，以便即使数据库的持久存储的某些部分已损坏，也可以使用数据库。

如果数据库已损坏（例如，在打开偏执检查时无法打开），可以使用 `leveldb::RepairDB` 函数来恢复尽可能多的数据。

## 近似大小 (Approximate Sizes)

`GetApproximateSizes` 方法可用于获取一个或多个键范围使用的文件系统空间的大约字节数。

```c++
leveldb::Range ranges[2];
ranges[0] = leveldb::Range("a", "c");
ranges[1] = leveldb::Range("x", "z");
uint64_t sizes[2];
db->GetApproximateSizes(ranges, 2, sizes);
```

前面的调用会将 `sizes[0]` 设置为键范围 `[a..c)` 使用的文件系统空间的大约字节数，并将 `sizes[1]` 设置为键范围 `[x..z)` 使用的大约字节数。

## 环境 (Environment)

leveldb 实现发出的所有文件操作（和其他操作系统调用）都通过 `leveldb::Env` 对象进行路由。复杂的客户端可能希望提供自己的 `Env` 实现以获得更好的控制。例如，应用程序可能会在文件 IO 路径中引入人为延迟，以限制 leveldb 对系统中其他活动的影响。

```c++
class SlowEnv : public leveldb::Env {
  ... Env 接口的实现 ...
};

SlowEnv env;
leveldb::Options options;
options.env = &env;
Status s = leveldb::DB::Open(options, ...);
```

## 移植 (Porting)

可以通过提供 `leveldb/port/port.h` 导出的类型/方法/函数的特定于平台的实现来将 leveldb 移植到新平台。有关更多详细信息，请参阅 `leveldb/port/port_example.h`。

此外，新平台可能需要一个新的默认 `leveldb::Env` 实现。有关示例，请参阅 `leveldb/util/env_posix.h`。

## 其他信息

有关 leveldb 实现的详细信息，请参阅以下文档：

1.  [实现说明](https://www.google.com/search?q=impl.md)
2.  [不可变表文件的格式](https://www.google.com/search?q=table_format.md)
3.  [日志文件的格式](https://www.google.com/search?q=log_format.md)

<!-- end list -->

```
```