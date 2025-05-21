leveldb File format
===================

    <beginning_of_file>
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block 1]
    ...
    [meta block K]
    [metaindex block]
    [index block]
    [Footer]        (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>

The file contains internal pointers.  Each such pointer is called
a BlockHandle and contains the following information:

    offset:   varint64
    size:     varint64

See [varints](https://developers.google.com/protocol-buffers/docs/encoding#varints)
for an explanation of varint64 format.

1.  The sequence of key/value pairs in the file are stored in sorted
order and partitioned into a sequence of data blocks.  These blocks
come one after another at the beginning of the file.  Each data block
is formatted according to the code in `block_builder.cc`, and then
optionally compressed.

2. After the data blocks we store a bunch of meta blocks.  The
supported meta block types are described below.  More meta block types
may be added in the future.  Each meta block is again formatted using
`block_builder.cc` and then optionally compressed.

3. A "metaindex" block.  It contains one entry for every other meta
block where the key is the name of the meta block and the value is a
BlockHandle pointing to that meta block.

4. An "index" block.  This block contains one entry per data block,
where the key is a string >= last key in that data block and before
the first key in the successive data block.  The value is the
BlockHandle for the data block.

5. At the very end of the file is a fixed length footer that contains
the BlockHandle of the metaindex and index blocks as well as a magic number.

        metaindex_handle: char[p];     // Block handle for metaindex
        index_handle:     char[q];     // Block handle for index
        padding:          char[40-p-q];// zeroed bytes to make fixed length
                                       // (40==2*BlockHandle::kMaxEncodedLength)
        magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)

## "filter" Meta Block

If a `FilterPolicy` was specified when the database was opened, a
filter block is stored in each table.  The "metaindex" block contains
an entry that maps from `filter.<N>` to the BlockHandle for the filter
block where `<N>` is the string returned by the filter policy's
`Name()` method.

The filter block stores a sequence of filters, where filter i contains
the output of `FilterPolicy::CreateFilter()` on all keys that are stored
in a block whose file offset falls within the range

    [ i*base ... (i+1)*base-1 ]

Currently, "base" is 2KB.  So for example, if blocks X and Y start in
the range `[ 0KB .. 2KB-1 ]`, all of the keys in X and Y will be
converted to a filter by calling `FilterPolicy::CreateFilter()`, and the
resulting filter will be stored as the first filter in the filter
block.

The filter block is formatted as follows:

    [filter 0]
    [filter 1]
    [filter 2]
    ...
    [filter N-1]

    [offset of filter 0]                  : 4 bytes
    [offset of filter 1]                  : 4 bytes
    [offset of filter 2]                  : 4 bytes
    ...
    [offset of filter N-1]                : 4 bytes

    [offset of beginning of offset array] : 4 bytes
    lg(base)                              : 1 byte

The offset array at the end of the filter block allows efficient
mapping from a data block offset to the corresponding filter.

## "stats" Meta Block

This meta block contains a bunch of stats.  The key is the name
of the statistic.  The value contains the statistic.

TODO(postrelease): record following stats.

    data size
    index size
    key size (uncompressed)
    value size (uncompressed)
    number of entries
    number of data blocks




leveldb 文件格式
===================

    <文件开头>
    [数据块 1]
    [数据块 2]
    ...
    [数据块 N]
    [元数据块 1]
    ...
    [元数据块 K]
    [元数据索引块]
    [索引块]
    [Footer]      (固定大小；从 file_size - sizeof(Footer) 处开始)
    <文件结尾>

文件包含内部指针。每个这样的指针称为一个 **BlockHandle**，并包含以下信息：

    offset:   varint64
    size:     varint64

关于 varint64 格式的解释，请参见 [varints](https://developers.google.com/protocol-buffers/docs/encoding#varints)。

---
1.  文件中的键/值对序列按排序顺序存储，并被分割成一系列数据块。这些块在文件开头一个接一个地排列。每个数据块根据 `block_builder.cc` 中的代码进行格式化，然后可选地进行压缩。

2.  在数据块之后，我们存储一系列元数据块。下面描述了支持的元数据块类型。将来可能会添加更多的元数据块类型。每个元数据块再次使用 `block_builder.cc` 进行格式化，然后可选地进行压缩。

3.  一个“**metaindex**”块。它为每个其他元数据块包含一个条目，其中键是元数据块的名称，值是指向该元数据块的 BlockHandle。

4.  一个“**index**”块。此块为每个数据块包含一个条目，其中键是一个字符串，该字符串 >= 该数据块中的最后一个键，并且 < 后续数据块中的第一个键。值是该数据块的 BlockHandle。

5.  在文件的最末端是一个固定长度的 **footer**，它包含元数据索引块和索引块的 BlockHandle 以及一个幻数 (magic number)。

        metaindex_handle: char[p];    // 元数据索引块的块句柄
        index_handle:     char[q];    // 索引块的块句柄
        padding:          char[40-p-q];// 用于补齐固定长度的零字节
                                      // (40==2*BlockHandle::kMaxEncodedLength)
        magic:            fixed64;    // == 0xdb4775248b80fb57 (小端字节序)

---
## “filter” 元数据块

如果在打开数据库时指定了 `FilterPolicy`，则每个表中都会存储一个过滤器块。“metaindex”块包含一个条目，该条目从 `filter.<N>` 映射到过滤器块的 BlockHandle，其中 `<N>` 是过滤器策略的 `Name()` 方法返回的字符串。

过滤器块存储一系列过滤器，其中过滤器 i 包含对所有存储在文件偏移量属于以下范围的块中的键调用 `FilterPolicy::CreateFilter()` 的输出：

    [ i*base ... (i+1)*base-1 ]

目前，“base”是 2KB。因此，例如，如果块 X 和 Y 的起始位置在 `[ 0KB .. 2KB-1 ]` 范围内，则 X 和 Y 中的所有键都将通过调用 `FilterPolicy::CreateFilter()` 转换为一个过滤器，并且生成的过滤器将作为第一个过滤器存储在过滤器块中。

过滤器块的格式如下：

    [过滤器 0]
    [过滤器 1]
    [过滤器 2]
    ...
    [过滤器 N-1]

    [过滤器 0 的偏移量]        : 4 字节
    [过滤器 1 的偏移量]        : 4 字节
    [过滤器 2 的偏移量]        : 4 字节
    ...
    [过滤器 N-1 的偏移量]       : 4 字节

    [偏移量数组起始处的偏移量] : 4 字节
    lg(base)                   : 1 字节

过滤器块末尾的偏移量数组允许从数据块偏移量到相应过滤器的高效映射。

---
## “stats” 元数据块

此元数据块包含一些统计信息。键是统计信息的名称。值包含统计信息。

TODO(postrelease): 记录以下统计信息。

    数据大小
    索引大小
    键大小 (未压缩)
    值大小 (未压缩)
    条目数
    数据块数