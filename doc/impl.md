## Files

The implementation of leveldb is similar in spirit to the representation of a
single [Bigtable tablet (section 5.3)](https://research.google/pubs/pub27898/).
However the organization of the files that make up the representation is
somewhat different and is explained below.

Each database is represented by a set of files stored in a directory. There are
several different types of files as documented below:

### Log files

A log file (*.log) stores a sequence of recent updates. Each update is appended
to the current log file. When the log file reaches a pre-determined size
(approximately 4MB by default), it is converted to a sorted table (see below)
and a new log file is created for future updates.

A copy of the current log file is kept in an in-memory structure (the
`memtable`). This copy is consulted on every read so that read operations
reflect all logged updates.

## Sorted tables

A sorted table (*.ldb) stores a sequence of entries sorted by key. Each entry is
either a value for the key, or a deletion marker for the key. (Deletion markers
are kept around to hide obsolete values present in older sorted tables).

The set of sorted tables are organized into a sequence of levels. The sorted
table generated from a log file is placed in a special **young** level (also
called level-0). When the number of young files exceeds a certain threshold
(currently four), all of the young files are merged together with all of the
overlapping level-1 files to produce a sequence of new level-1 files (we create
a new level-1 file for every 2MB of data.)

Files in the young level may contain overlapping keys. However files in other
levels have distinct non-overlapping key ranges. Consider level number L where
L >= 1. When the combined size of files in level-L exceeds (10^L) MB (i.e., 10MB
for level-1, 100MB for level-2, ...), one file in level-L, and all of the
overlapping files in level-(L+1) are merged to form a set of new files for
level-(L+1). These merges have the effect of gradually migrating new updates
from the young level to the largest level using only bulk reads and writes
(i.e., minimizing expensive seeks).

### Manifest

A MANIFEST file lists the set of sorted tables that make up each level, the
corresponding key ranges, and other important metadata. A new MANIFEST file
(with a new number embedded in the file name) is created whenever the database
is reopened. The MANIFEST file is formatted as a log, and changes made to the
serving state (as files are added or removed) are appended to this log.

### Current

CURRENT is a simple text file that contains the name of the latest MANIFEST
file.

### Info logs

Informational messages are printed to files named LOG and LOG.old.

### Others

Other files used for miscellaneous purposes may also be present (LOCK, *.dbtmp).

## Level 0

When the log file grows above a certain size (4MB by default):
Create a brand new memtable and log file and direct future updates here.

In the background:

1. Write the contents of the previous memtable to an sstable.
2. Discard the memtable.
3. Delete the old log file and the old memtable.
4. Add the new sstable to the young (level-0) level.

## Compactions

When the size of level L exceeds its limit, we compact it in a background
thread. The compaction picks a file from level L and all overlapping files from
the next level L+1. Note that if a level-L file overlaps only part of a
level-(L+1) file, the entire file at level-(L+1) is used as an input to the
compaction and will be discarded after the compaction.  Aside: because level-0
is special (files in it may overlap each other), we treat compactions from
level-0 to level-1 specially: a level-0 compaction may pick more than one
level-0 file in case some of these files overlap each other.

A compaction merges the contents of the picked files to produce a sequence of
level-(L+1) files. We switch to producing a new level-(L+1) file after the
current output file has reached the target file size (2MB). We also switch to a
new output file when the key range of the current output file has grown enough
to overlap more than ten level-(L+2) files.  This last rule ensures that a later
compaction of a level-(L+1) file will not pick up too much data from
level-(L+2).

The old files are discarded and the new files are added to the serving state.

Compactions for a particular level rotate through the key space. In more detail,
for each level L, we remember the ending key of the last compaction at level L.
The next compaction for level L will pick the first file that starts after this
key (wrapping around to the beginning of the key space if there is no such
file).

Compactions drop overwritten values. They also drop deletion markers if there
are no higher numbered levels that contain a file whose range overlaps the
current key.

### Timing

Level-0 compactions will read up to four 1MB files from level-0, and at worst
all the level-1 files (10MB). I.e., we will read 14MB and write 14MB.

Other than the special level-0 compactions, we will pick one 2MB file from level
L. In the worst case, this will overlap ~ 12 files from level L+1 (10 because
level-(L+1) is ten times the size of level-L, and another two at the boundaries
since the file ranges at level-L will usually not be aligned with the file
ranges at level-L+1). The compaction will therefore read 26MB and write 26MB.
Assuming a disk IO rate of 100MB/s (ballpark range for modern drives), the worst
compaction cost will be approximately 0.5 second.

If we throttle the background writing to something small, say 10% of the full
100MB/s speed, a compaction may take up to 5 seconds. If the user is writing at
10MB/s, we might build up lots of level-0 files (~50 to hold the 5*10MB). This
may significantly increase the cost of reads due to the overhead of merging more
files together on every read.

Solution 1: To reduce this problem, we might want to increase the log switching
threshold when the number of level-0 files is large. Though the downside is that
the larger this threshold, the more memory we will need to hold the
corresponding memtable.

Solution 2: We might want to decrease write rate artificially when the number of
level-0 files goes up.

Solution 3: We work on reducing the cost of very wide merges. Perhaps most of
the level-0 files will have their blocks sitting uncompressed in the cache and
we will only need to worry about the O(N) complexity in the merging iterator.

### Number of files

Instead of always making 2MB files, we could make larger files for larger levels
to reduce the total file count, though at the expense of more bursty
compactions.  Alternatively, we could shard the set of files into multiple
directories.

An experiment on an ext3 filesystem on Feb 04, 2011 shows the following timings
to do 100K file opens in directories with varying number of files:


| Files in directory | Microseconds to open a file |
|-------------------:|----------------------------:|
|               1000 |                           9 |
|              10000 |                          10 |
|             100000 |                          16 |

So maybe even the sharding is not necessary on modern filesystems?

## Recovery

* Read CURRENT to find name of the latest committed MANIFEST
* Read the named MANIFEST file
* Clean up stale files
* We could open all sstables here, but it is probably better to be lazy...
* Convert log chunk to a new level-0 sstable
* Start directing new writes to a new log file with recovered sequence#

## Garbage collection of files

`RemoveObsoleteFiles()` is called at the end of every compaction and at the end
of recovery. It finds the names of all files in the database. It deletes all log
files that are not the current log file. It deletes all table files that are not
referenced from some level and are not the output of an active compaction.


## 文件

LevelDB 的实现精神上类似于单个 [Bigtable tablet（第 5.3 节）](https://research.google/pubs/pub27898/) 的表示。然而，构成该表示的文件组织方式有所不同，具体如下所述。

每个数据库由存储在目录中的一组文件表示。如下文所述，存在几种不同类型的文件：

### 日志文件

日志文件（*.log）存储一系列最近的更新。每次更新都会追加到当前的日志文件中。当日志文件达到预定大小时（默认为约 4MB），它将被转换为一个排序表（见下文），并为将来的更新创建一个新的日志文件。

当前日志文件的一个副本保存在内存结构（`memtable`）中。每次读取都会查询此副本，以便读取操作反映所有已记录的更新。

## 排序表

排序表（*.ldb）存储按键排序的一系列条目。每个条目要么是键的值，要么是键的删除标记。（保留删除标记是为了隐藏旧排序表中存在的过时值）。

排序表集合被组织成一系列层级。从日志文件生成的排序表被放置在一个特殊的**年轻**层级（也称为 level-0）。当年文件数量超过某个阈值（目前为四个）时，所有年轻文件将与所有重叠的 level-1 文件合并，以生成一系列新的 level-1 文件（我们为每 2MB 数据创建一个新的 level-1 文件）。

年轻层级中的文件可能包含重叠的键。但是，其他层级中的文件具有明确的不重叠键范围。考虑层级编号 L，其中 L >= 1。当 L 层中文件的总大小超过 (10^L) MB（即 level-1 为 10MB，level-2 为 100MB，...）时，L 层中的一个文件以及 level-(L+1) 中所有重叠的文件将被合并，形成 level-(L+1) 的一组新文件。这些合并的效果是仅使用批量读写（即最小化昂贵的寻道操作）逐渐将新的更新从年轻层级迁移到最大层级。

### MANIFEST 文件

MANIFEST 文件列出了构成每个层级的排序表集合、相应的键范围以及其他重要的元数据。每当重新打开数据库时，都会创建一个新的 MANIFEST 文件（文件名中嵌入新的编号）。MANIFEST 文件格式化为日志，对服务状态所做的更改（随着文件的添加或删除）将追加到此日志中。

### Current 文件

CURRENT 是一个简单的文本文件，包含最新 MANIFEST 文件的名称。

### 信息日志

信息性消息将打印到名为 LOG 和 LOG.old 的文件中。

### 其他文件

还可能存在用于其他杂项目的的文件（LOCK, *.dbtmp）。

## Level 0

当日志文件增长超过一定大小时（默认为 4MB）：
创建一个全新的 memtable 和日志文件，并将未来的更新定向到此处。

在后台：

1. 将先前 memtable 的内容写入 sstable。
2. 丢弃该 memtable。
3. 删除旧的日志文件和旧的 memtable。
4. 将新的 sstable 添加到年轻（level-0）层级。

## 压缩 (Compactions)

当 L 层的大小超过其限制时，我们会在后台线程中对其进行压缩。压缩过程会选择 L 层的一个文件以及下一层 L+1 中所有重叠的文件。请注意，如果一个 level-L 文件仅与 level-(L+1) 文件的一部分重叠，则 level-(L+1) 的整个文件都将用作压缩的输入，并在压缩后被丢弃。另外：因为 level-0 很特殊（其中的文件可能相互重叠），我们对从 level-0 到 level-1 的压缩进行特殊处理：如果某些 level-0 文件相互重叠，则 level-0 压缩可能会选择多个 level-0 文件。

压缩操作会合并所选文件的内容，以生成一系列 level-(L+1) 文件。当当前输出文件达到目标文件大小（2MB）后，我们会切换到生成一个新的 level-(L+1) 文件。当当前输出文件的键范围增长到足以与超过十个 level-(L+2) 文件重叠时，我们也会切换到一个新的输出文件。这最后一条规则确保了后续 level-(L+1) 文件的压缩不会从 level-(L+2) 中拾取过多的数据。

旧文件将被丢弃，新文件将被添加到服务状态中。

特定层级的压缩会轮换遍历键空间。更详细地说，对于每个层级 L，我们会记住该层级上一次压缩的结束键。L 层的下一次压缩将选择在此键之后开始的第一个文件（如果没有这样的文件，则回绕到键空间的开头）。

压缩会丢弃被覆盖的值。如果不存在其范围与当前键重叠的更高编号层级的文件，它们也会丢弃删除标记。

### 时间安排

Level-0 压缩最多会从 level-0 读取四个 1MB 的文件，最坏情况下会读取所有 level-1 文件（10MB）。也就是说，我们将读取 14MB 并写入 14MB。

除了特殊的 level-0 压缩外，我们将从 L 层选取一个 2MB 的文件。在最坏的情况下，这将与 level L+1 中的约 12 个文件重叠（10 个是因为 level-(L+1) 是 level-L 大小的十倍，另外两个在边界处，因为 level-L 的文件范围通常不会与 level-L+1 的文件范围对齐）。因此，压缩将读取 26MB 并写入 26MB。假设磁盘 IO 速率为 100MB/s（现代驱动器的大致范围），最坏的压缩成本约为 0.5 秒。

如果我们将后台写入限制在一个较小的值，例如完整 100MB/s 速度的 10%，则一次压缩可能需要长达 5 秒。如果用户以 10MB/s 的速度写入，我们可能会累积大量 level-0 文件（约 50 个以容纳 5*10MB 的数据）。这可能会由于每次读取时合并更多文件的开销而显着增加读取成本。

解决方案 1：为了减少这个问题，当 level-0 文件数量较多时，我们可能需要增加日志切换阈值。不过缺点是，这个阈值越大，我们就需要越多的内存来容纳相应的 memtable。

解决方案 2：当 level-0 文件数量增加时，我们可能需要人为地降低写入速率。

解决方案 3：我们致力于降低极宽合并的成本。也许大多数 level-0 文件的块将以未压缩的形式存在于缓存中，我们只需要担心合并迭代器中的 O(N) 复杂度。

### 文件数量

我们可以为更大的层级制作更大的文件，而不是总是制作 2MB 的文件，以减少总文件数量，尽管这会以更具突发性的压缩为代价。或者，我们可以将文件集分片到多个目录中。

2011 年 2 月 4 日在 ext3 文件系统上进行的一项实验显示了在不同文件数量的目录中打开 10 万个文件的以下时间：

| 目录中的文件数 | 打开一个文件所需的微秒数 |
| -----------------: | --------------------------: |
|             1000 |                             9 |
|            10000 |                            10 |
|           100000 |                            16 |

那么，也许在现代文件系统上甚至不需要分片？

## 恢复

* 读取 CURRENT 以查找最新提交的 MANIFEST 的名称
* 读取指定的 MANIFEST 文件
* 清理过时的文件
* 我们可以在这里打开所有 sstable，但最好是惰性加载...
* 将日志块转换为新的 level-0 sstable
* 开始将新的写入定向到具有已恢复序列号的新日志文件

## 文件垃圾回收

`RemoveObsoleteFiles()` 会在每次压缩结束时以及恢复结束时调用。它会查找数据库中所有文件的名称。它会删除所有不是当前日志文件的日志文件。它会删除所有未被任何层级引用且不是活动压缩输出的表文件。