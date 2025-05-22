// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// (写操作需要外部同步，通常是一个互斥锁。)
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
// (读操作需要保证在读取过程中 SkipList 不会被销毁。除此之外，读操作无需任何内部锁定或同步即可进行。)
//
// Invariants:
// (不变性约束：)
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
// (已分配的节点在 SkipList 被销毁之前永远不会被删除。这一点由代码简单保证，因为我们从不删除任何跳表节点。)
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
// (节点的内容（除了 next/prev 指针）在节点被链接到 SkipList 后是不可变的。
// 只有 Insert() 方法会修改列表，并且它会小心地初始化节点并使用释放存储（release-stores）来发布这些节点到一个或多个链表中。)
//
// ... prev vs. next pointer ordering ... (关于 prev 和 next 指针顺序的说明)

#include <atomic>  // 用于 std::atomic，实现原子操作
#include <cassert> // 用于 assert()，进行断言检查
#include <cstdlib> // 用于 rand() 等标准库函数 (虽然这里用的是自定义的 Random)

#include "util/arena.h"    // 用于 Arena 内存分配器
#include "util/random.h"   // 用于生成随机数，决定节点高度

namespace leveldb {

// SkipList 类模板，Key 是键的类型，Comparator 是比较器类
template <typename Key, class Comparator>
class SkipList {
 private:
  // 跳表节点的内部结构定义
  struct Node;

 public:
  // 构造函数：创建一个新的 SkipList 对象。
  // 使用 "cmp" 进行键比较，并使用 "*arena" 分配内存。
  // 在 arena 中分配的对象必须在 skiplist 对象的生命周期内保持分配状态。
  explicit SkipList(Comparator cmp, Arena* arena);

  // 禁用拷贝构造函数和拷贝赋值运算符，防止意外拷贝导致问题。
  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // 将键插入到列表中。
  // 要求：列表中当前没有任何与 key 相等的条目。
  void Insert(const Key& key);

  // 如果列表中存在与 key 相等的条目，则返回 true。
  bool Contains(const Key& key) const;

  // 用于遍历跳表内容的迭代器类
  class Iterator {
   public:
    // 初始化一个指定列表的迭代器。
    // 返回的迭代器初始时是无效的。
    explicit Iterator(const SkipList* list);

    // 如果迭代器指向一个有效的节点，则返回 true。
    bool Valid() const;

    // 返回当前位置的键。
    // 要求：Valid() 为 true。
    const Key& key() const;

    // 前进到下一个位置。
    // 要求：Valid() 为 true。
    void Next();

    // 后退到上一个位置。
    // 要求：Valid() 为 true。
    void Prev();

    // 前进到第一个键 >= target 的条目。
    void Seek(const Key& target);

    // 定位到列表中的第一个条目。
    // 如果列表不为空，迭代器的最终状态为 Valid()。
    void SeekToFirst();

    // 定位到列表中的最后一个条目。
    // 如果列表不为空，迭代器的最终状态为 Valid()。
    void SeekToLast();

   private:
    const SkipList* list_; // 指向所属的 SkipList
    Node* node_;           // 当前指向的节点
    // Intentionally copyable (迭代器设计为可拷贝的)
  };

 private:
  // 跳表节点的最大高度（层数）
  enum { kMaxHeight = 12 };

  // 获取当前跳表的最大高度。
  // 使用 relaxed 内存序，因为即使读到旧值也是可接受的。
  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  // 创建一个具有指定键和高度的新节点。
  Node* NewNode(const Key& key, int height);
  // 生成一个随机的高度，用于新插入的节点。
  int RandomHeight();
  // 比较两个键是否相等。
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // 如果 key 大于节点 "n" 中存储的数据，则返回 true。
  // n 为 nullptr 时被认为是无穷大。
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // 返回第一个键大于或等于 key 的节点。
  // 如果没有这样的节点，则返回 nullptr。
  // 如果 prev 非空，则用指向每一层 [0..max_height_-1] 中前一个节点的指针填充 prev[level]。
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // 返回键小于 key 的最后一个节点。
  // 如果没有这样的节点，则返回 head_。
  Node* FindLessThan(const Key& key) const;

  // 返回列表中的最后一个节点。
  // 如果列表为空，则返回 head_。
  Node* FindLast() const;

  // 以下成员在构造后不可变
  Comparator const compare_; // 用于比较键的比较器对象
  Arena* const arena_;      // 用于节点分配的 Arena 对象

  Node* const head_; // 跳表的头节点（哨兵节点）

  // 仅由 Insert() 修改。读取者可以并发地读取（racily），但旧值是可接受的。
  std::atomic<int> max_height_;  // 整个列表的当前最大高度

  // 仅由 Insert() 读写。
  Random rnd_; // 用于生成随机高度的随机数生成器
};

// 实现细节如下

// SkipList 节点 Node 的结构定义
// 该结构包含一个键和一个指向下一个节点的指针数组。
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  // 构造函数，初始化节点的键
  explicit Node(const Key& k) : key(k) {}

  Key const key; // 节点存储的键，不可变

  // 访问和修改链接的方法。封装在方法中，以便根据需要添加适当的内存屏障。
  Node* Next(int n) {
    assert(n >= 0); //确保层数有效
    // 使用 'acquire load'（获取加载），以便观察到返回节点的完全初始化版本。
    // 这是为了确保在读取 next 指针时，所指向节点的内容对当前线程可见。
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0); //确保层数有效
    // 使用 'release store'（释放存储），以便任何通过此指针读取的线程都能观察到插入节点的完全初始化版本。
    // 这是为了确保在设置 next 指针后，新节点的内容对其他线程可见。
    next_[n].store(x, std::memory_order_release);
  }

  // 无内存屏障的变体，可以在少数安全的位置使用（例如，在持有锁或已知单线程操作时）。
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // 长度等于节点高度的指针数组。next_[0] 是最低层的链接。
  // 使用 std::atomic<Node*> 来确保对指针的读写是原子的，配合内存序来保证线程安全。
  // 这里的 '1' 只是一个占位符，实际大小在 NewNode 中动态确定（柔性数组成员技巧的变体）。
  std::atomic<Node*> next_[1];
};

// 创建一个新节点
// 接收一个键 key 和一个高度 height。
// 计算出一个高度为 height 的 Node 对象（包含其柔性数组成员 next_）所需要的总内存大小。
// 使用 Arena 对象 (arena_) 的 AllocateAligned 方法分配这块对齐的原始内存，并将地址存储在常量指针 node_memory 中。
// 使用 placement new 在 node_memory 指向的已分配内存上构造一个新的 Node 对象，并用传入的 key 初始化它。
// 返回指向这个新构造的 Node 对象的指针。
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  // 计算节点所需的总内存：Node 结构本身的大小加上额外的高度层所需的指针空间。
  // (height - 1) 是因为 Node 结构中已经包含了一个 next_ 指针的空间。
  char* const node_memory = 
  ->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  // 使用 placement new 在已分配的内存 (node_memory) 上构造 Node 对象。
  return new (node_memory) Node(key);
}


// 迭代器构造函数
template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr; // 初始时迭代器无效
}

// 检查迭代器是否有效
template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

// 获取迭代器当前位置的键
template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid()); // 必须有效才能获取键
  return node_->key;
}

// 迭代器前进到下一个节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0); // 移动到最低层的下一个节点
}

// 迭代器后退到上一个节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // 跳表通常不直接存储 prev 指针以节省空间。
  // 这里通过从头开始搜索小于当前节点键值的最后一个节点来实现 Prev()。
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) { // 如果找到的是头节点，说明没有更小的了
    node_ = nullptr; // 迭代器变为无效
  }
}

// 迭代器定位到第一个键大于或等于 target 的节点
// 就是不需要填充 prev 数组的 FindGreaterOrEqual 方法
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

// 迭代器定位到跳表的第一个实际节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0); // 头节点的下一个节点即为第一个
}

// 迭代器定位到跳表的最后一个实际节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) { // 如果找到的是头节点，说明列表为空
    node_ = nullptr;
  }
}

// 生成随机高度
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // 以 1/kBranching 的概率增加高度
  static const unsigned int kBranching = 4; // 分支因子，通常为 4
  int height = 1; // 最小高度为 1
  // 循环增加高度，直到达到最大高度或随机判定不再增加
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// 检查 key 是否在节点 n 之后（即 key > n->key）
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // n 为 nullptr 被认为是无穷大，所以任何 key 都不在其后
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 查找第一个键大于或等于 key 的节点
// 若prev 非空，则填充 prev 数组，prev[level] 指向当前层的前驱节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_; // 从头节点开始查找
  int level = GetMaxHeight() - 1; // 从最高层开始
  while (true) {
    Node* next = x->Next(level); // 当前层 x 的下一个节点
    if (KeyIsAfterNode(key, next)) {
      // key 在 next 节点之后，意味着应该继续在当前层的这个方向上搜索
      x = next;
    } else {
      // key 小于或等于 next 节点，或者 next 是 nullptr
      // 记录 prev 数组（如果需要的话），用于后续插入操作
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        // 如果已经在最底层，则 next 就是目标节点（或者 nullptr）
        return next;
      } else {
        // 否则，下降到下一层继续搜索
        level--;
      }
    }
  }
}

// 查找键小于 key 的最后一个节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_; // 从头节点开始
  int level = GetMaxHeight() - 1; // 从最高层开始
  while (true) {
    // 不变性：x 要么是头节点，要么 x->key < key
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      // next 是 nullptr 或者 next->key >= key
      // 这意味着在当前层，x 是最后一个小于 key 的节点
      if (level == 0) {
        // 如果在最底层，x 就是最终结果
        return x;
      } else {
        // 否则，下降到下一层继续精确查找
        level--;
      }
    } else {
      // next->key < key，继续在当前层向右移动
      x = next;
    }
  }
}

// 查找列表中的最后一个节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_; // 从头节点开始
  int level = GetMaxHeight() - 1; // 从最高层开始
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      // 当前层的末尾
      if (level == 0) {
        // 如果在最底层，x 就是最后一个节点（如果列表为空，则 x 是 head_）
        return x;
      } else {
        // 否则，下降到下一层继续查找
        level--;
      }
    } else {
      // 继续在当前层向右移动
      x = next;
    }
  }
}

// SkipList 构造函数
template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp), // 初始化比较器
      arena_(arena),   // 初始化 Arena
      // 创建头节点，键可以是任意值（这里用0），高度为 kMaxHeight
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1), // 初始时列表的最大高度为 1 (只有头节点)
      rnd_(0xdeadbeef) { // 初始化随机数生成器，使用固定的种子
  // 初始化头节点的所有层级的 next 指针为 nullptr
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

// 插入操作
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // 注意：这里的 FindGreaterOrEqual 可以使用无屏障版本，
  // 因为 Insert() 操作本身需要外部同步（例如互斥锁）。
  Node* prev[kMaxHeight]; // 用于存储插入位置每一层的前驱节点
  Node* x = FindGreaterOrEqual(key, prev); // 查找插入位置，并填充 prev 数组

  // 跳表结构不允许重复插入键值相等的节点
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight(); // 为新节点生成一个随机高度
  if (height > GetMaxHeight()) { // 如果新节点的高度超过了当前列表的最大高度
    // 更新 prev 数组中超出原最大高度的部分，使其指向头节点
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // 更新列表的 max_height_。
    // 这里可以不使用任何同步机制与并发读取者同步。
    // 一个并发读取者如果观察到新的 max_height_ 值，它要么看到 head_ 节点
    // 在新层级上的旧指针值 (nullptr)，要么看到下面循环中设置的新值。
    // 在前一种情况下，读取者会因为 nullptr 比所有键都大而立即降到下一层。
    // 在后一种情况下，读取者会使用新的节点。
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = NewNode(key, height); // 创建新节点
  // 将新节点链接到跳表中
  for (int i = 0; i < height; i++) {
    // 使用 NoBarrier_SetNext 和 NoBarrier_Next 是安全的，因为我们将在
    // 将指向 "x" 的指针发布到 prev[i] 时添加一个屏障 (通过 prev[i]->SetNext(i, x))。
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i)); // 新节点的 next 指向 prev[i] 原来的 next
    prev[i]->SetNext(i, x); // prev[i] 的 next 指向新节点 x (这里有 release store)
  }
}

// 检查是否包含指定的键
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr); // 查找大于或等于 key 的节点
  // 如果找到了节点 x，并且 x 的键与 key 相等
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_