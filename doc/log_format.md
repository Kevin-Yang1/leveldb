leveldb Log format
==================
The log file contents are a sequence of 32KB blocks.  The only exception is that
the tail of the file may contain a partial block.

Each block consists of a sequence of records:

    block := record* trailer?
    record :=
      checksum: uint32     // crc32c of type and data[] ; little-endian
      length: uint16       // little-endian
      type: uint8          // One of FULL, FIRST, MIDDLE, LAST
      data: uint8[length]

A record never starts within the last six bytes of a block (since it won't fit).
Any leftover bytes here form the trailer, which must consist entirely of zero
bytes and must be skipped by readers.

Aside: if exactly seven bytes are left in the current block, and a new non-zero
length record is added, the writer must emit a FIRST record (which contains zero
bytes of user data) to fill up the trailing seven bytes of the block and then
emit all of the user data in subsequent blocks.

More types may be added in the future.  Some Readers may skip record types they
do not understand, others may report that some data was skipped.

    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4

The FULL record contains the contents of an entire user record.

FIRST, MIDDLE, LAST are types used for user records that have been split into
multiple fragments (typically because of block boundaries).  FIRST is the type
of the first fragment of a user record, LAST is the type of the last fragment of
a user record, and MIDDLE is the type of all interior fragments of a user
record.

Example: consider a sequence of user records:

    A: length 1000
    B: length 97270
    C: length 8000

**A** will be stored as a FULL record in the first block.

**B** will be split into three fragments: first fragment occupies the rest of
the first block, second fragment occupies the entirety of the second block, and
the third fragment occupies a prefix of the third block.  This will leave six
bytes free in the third block, which will be left empty as the trailer.

**C** will be stored as a FULL record in the fourth block.

----

## Some benefits over the recordio format:

1. We do not need any heuristics for resyncing - just go to next block boundary
   and scan.  If there is a corruption, skip to the next block.  As a
   side-benefit, we do not get confused when part of the contents of one log
   file are embedded as a record inside another log file.

2. Splitting at approximate boundaries (e.g., for mapreduce) is simple: find the
   next block boundary and skip records until we hit a FULL or FIRST record.

3. We do not need extra buffering for large records.

## Some downsides compared to recordio format:

1. No packing of tiny records.  This could be fixed by adding a new record type,
   so it is a shortcoming of the current implementation, not necessarily the
   format.

2. No compression.  Again, this could be fixed by adding new record types.



leveldb 日志格式
==================

日志文件内容是一系列 32KB 大小的块。唯一的例外是文件末尾可能包含一个部分块。

每个块由一系列记录组成：

    块 := 记录* 尾部?
    记录 :=
      校验和: uint32   // type 和 data[] 的 crc32c 校验和；小端字节序
      长度: uint16     // 小端字节序
      类型: uint8      // FULL、FIRST、MIDDLE、LAST 中的一种
      数据: uint8[长度]

一条记录永远不会在块的最后六个字节内开始（因为它放不下）。此处任何剩余的字节构成尾部，尾部必须完全由零字节组成，并且读取器必须跳过它。

另外：如果当前块中正好剩下七个字节，并且添加了一条新的非零长度记录，则写入器必须发出一条 FIRST 记录（包含零字节的用户数据）以填充块的尾部七个字节，然后在后续块中发出所有用户数据。

将来可能会添加更多类型。一些读取器可能会跳过它们不理解的记录类型，另一些读取器可能会报告某些数据被跳过。

    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4

FULL 记录包含整个用户记录的内容。

FIRST、MIDDLE、LAST 是用于已被拆分成多个片段（通常是由于块边界）的用户记录的类型。FIRST 是用户记录第一个片段的类型，LAST 是用户记录最后一个片段的类型，MIDDLE 是用户记录所有内部片段的类型。

示例：考虑一系列用户记录：

    A: 长度 1000
    B: 长度 97270
    C: 长度 8000

**A** 将作为一条 FULL 记录存储在第一个块中。

**B** 将被拆分为三个片段：第一个片段占据第一个块的剩余部分，第二个片段占据整个第二个块，第三个片段占据第三个块的前缀部分。这将在第三个块中留下六个字节的空闲空间，这些空间将作为尾部留空。

**C** 将作为一条 FULL 记录存储在第四个块中。

----

## 相较于 recordio 格式的一些优点：

1.  我们不需要任何启发式方法来进行重新同步——只需转到下一个块边界并扫描。如果存在损坏，则跳到下一个块。作为附带的好处，当一个日志文件的部分内容作为记录嵌入到另一个日志文件中时，我们不会混淆。

2.  在近似边界处进行拆分（例如，对于 mapreduce）很简单：找到下一个块边界并跳过记录，直到我们遇到 FULL 或 FIRST 记录。

3.  对于大记录，我们不需要额外的缓冲。

## 相较于 recordio 格式的一些缺点：

1.  没有对小记录进行打包。这可以通过添加新的记录类型来修复，因此这是当前实现的缺点，而不一定是格式本身的缺点。

2.  没有压缩。同样，这可以通过添加新的记录类型来修复。