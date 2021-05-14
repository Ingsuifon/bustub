//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // 1. 获取header_page，设置它的元数据
  Page *newpage = buffer_pool_manager_->NewPageImpl(&header_page_id_);
  // 锁住headerPage
  newpage->WLatch();
  auto header = reinterpret_cast<HashTableHeaderPage *>(newpage->GetData());
  header->SetPageId(header_page_id_);
  size_t num_blocks = (num_buckets - 1) / BLOCK_ARRAY_SIZE + 1;
  header->SetSize(num_blocks * BLOCK_ARRAY_SIZE);
  // 2. 获取block_page
  page_id_t tmpPageId;
  for (size_t i = 0; i < num_blocks; i++) {
    buffer_pool_manager_->NewPageImpl(&tmpPageId);
    // 没有可用的Page就等
    if (tmpPageId == INVALID_PAGE_ID) {
      i--;
      continue;
    }
    header->AddBlockPageId(tmpPageId);
    buffer_pool_manager_->UnpinPageImpl(tmpPageId, false);
  }
  newpage->WUnlatch();
  buffer_pool_manager_->UnpinPageImpl(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  // 1. 获取header_page
  Page* _head = buffer_pool_manager_->FetchPageImpl(header_page_id_);
  _head->RLatch();
  auto head = reinterpret_cast<HashTableHeaderPage *>(_head->GetData());
  // 2.计算哈希值，计算slot的位置
  uint64_t hashcode = hash_fn_.GetHash(key);
  slot_offset_t slot_idx = hashcode % head->GetSize();
  size_t block_idx = slot_idx / BLOCK_ARRAY_SIZE;
  slot_offset_t slot = slot_idx % BLOCK_ARRAY_SIZE;
  // 3.获取对应的block_page
  Page* _block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
  _block->RLatch();
  HASH_TABLE_BLOCK_TYPE* block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
  // 4.获取value
  while (block->IsOccupied(slot)) {
    // 先判断当前slot是否存放键值对，然后判断存放的key是否为目标key
    if (block->IsReadable(slot) && comparator_(key, block->KeyAt(slot)) == 0)
      result->emplace_back(block->ValueAt(slot));
    slot++;
    // 如果又回到了起点，说明已经遍历完了整个哈希表
    if (block_idx * BLOCK_ARRAY_SIZE + slot == slot_idx) break;
    // 已经到了当前block_page的末端，释放当前block_page并获取下一个block_page
    if (slot == BLOCK_ARRAY_SIZE) {
      _block->RUnlatch();
      slot = 0;
      buffer_pool_manager_->UnpinPage(head->GetBlockPageId(block_idx++), false);
      if (block_idx == head->NumBlocks()) block_idx = 0;
      _block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
      _block->RLatch();
      block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
    }
  } 
  // 5.释放Page
  _block->RUnlatch();
  buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), false);
  _head->RUnlatch();
  buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
  table_latch_.RUnlock();
  return result->size() > 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  // 1. 获取header_page
  Page *_head = buffer_pool_manager_->FetchPageImpl(header_page_id_);
  _head->RLatch();
  auto head = reinterpret_cast<HashTableHeaderPage *>(_head->GetData());
  // 2.计算哈希值，计算slot的位置
  uint64_t hashcode = hash_fn_.GetHash(key);
  slot_offset_t slot_idx = hashcode % head->GetSize();
  size_t block_idx = slot_idx / BLOCK_ARRAY_SIZE;
  slot_offset_t slot = slot_idx % BLOCK_ARRAY_SIZE;
  // 3.获取对应的block_page
  Page *_block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
  _block->WLatch();
  HASH_TABLE_BLOCK_TYPE *block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
  // 4.插入键值对
  while (!block->Insert(slot, key, value)) {
    if (comparator_(key, block->KeyAt(slot)) == 0 && value == block->ValueAt(slot)) {
      _block->WUnlatch();
      buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), false);
      _head->RUnlatch();
      buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
      table_latch_.RUnlock();
      return false;
    }
    slot++;
    if (slot == BLOCK_ARRAY_SIZE) {
      _block->WUnlatch();
      slot = 0;
      buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx++), false);
      if (block_idx == head->NumBlocks()) block_idx = 0;
      _block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
      _block->WLatch();
      block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
    }
    // 哈希表已满，需要resize
    if (block_idx * BLOCK_ARRAY_SIZE + slot == slot_idx) {
      // 释放Page
      _block->WUnlatch();
      buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), false);
      _head->RUnlatch();
      buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
      Resize(GetSize());
      // 重新获取header_page
      _head = buffer_pool_manager_->FetchPageImpl(header_page_id_);
      _head->RLatch();
      head = reinterpret_cast<HashTableHeaderPage *>(_head->GetData());
      // 重新计算哈希值，计算slot
      hashcode = hash_fn_.GetHash(key);
      slot_idx = hashcode % head->GetSize();
      block_idx = slot_idx / BLOCK_ARRAY_SIZE;
      slot = slot_idx % BLOCK_ARRAY_SIZE;
      // 重新获取block_page
      _block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
      _block->WLatch();
      block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
    }
  }
  _block->WUnlatch();
  buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), true);
  _head->RUnlatch();
  buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
  table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  // 1. 获取header_page
  Page *_head = buffer_pool_manager_->FetchPageImpl(header_page_id_);
  _head->RLatch();
  auto head = reinterpret_cast<HashTableHeaderPage *>(_head->GetData());
  // 2.计算哈希值，计算slot的位置
  uint64_t hashcode = hash_fn_.GetHash(key);
  slot_offset_t slot_idx = hashcode % head->GetSize();
  size_t block_idx = slot_idx / BLOCK_ARRAY_SIZE;
  slot_offset_t slot = slot_idx % BLOCK_ARRAY_SIZE;
  // 3.获取对应的block_page
  Page *_block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
  _block->WLatch();
  HASH_TABLE_BLOCK_TYPE *block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
  // 4.删除键值对
  // 如果这个slot从来就没有插入过值，说明哈希表根本不可能有待删除的键值对
  if (!block->IsOccupied(slot)) {
    _block->WUnlatch();
    buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), false);
    _head->RUnlatch();
    buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  while (!block->IsReadable(slot) || !(comparator_(block->KeyAt(slot), key) == 0) || !(block->ValueAt(slot) == value)) {
    slot++;
    if (slot == BLOCK_ARRAY_SIZE) {
      _block->WUnlatch();
      slot = 0;
      buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx++), false);
      if (block_idx == head->NumBlocks()) block_idx = 0;
      _block = buffer_pool_manager_->FetchPageImpl(head->GetBlockPageId(block_idx));
      _block->WLatch();
      block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
    }
    if (block_idx * BLOCK_ARRAY_SIZE + slot == slot_idx) {
      _block->WUnlatch();
      buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), false);
      _head->RUnlatch();
      buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
      table_latch_.RUnlock();
      return false;
    }   
  }
  block->Remove(slot);
  _block->WUnlatch();
  buffer_pool_manager_->UnpinPageImpl(head->GetBlockPageId(block_idx), true);
  _head->RUnlatch();
  buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
  table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  // 1.求新哈希表的block数量
  size_t num_buckets = (2 * initial_size - 1) / BLOCK_ARRAY_SIZE + 1;
  // 2.记录旧哈希表的headPage_id
  page_id_t old_header_id = header_page_id_;
  // 3.分配新哈希表的headPage
  Page* new_head_page = buffer_pool_manager_->NewPageImpl(&header_page_id_);
  auto new_header = reinterpret_cast<HashTableHeaderPage *>(new_head_page->GetData());
  // 4.分配新哈希表的blockPage
  page_id_t temp_block_page_id;
  for (size_t i = 0; i < num_buckets; i++) {
    buffer_pool_manager_->NewPageImpl(&temp_block_page_id);
    if (temp_block_page_id == INVALID_PAGE_ID) {
      i--;
      continue;
    }
    new_header->AddBlockPageId(temp_block_page_id);
    buffer_pool_manager_->UnpinPageImpl(temp_block_page_id, false);
  }
  // 5.旧哈希表的元素搬家
  Page* old_head_page = buffer_pool_manager_->FetchPageImpl(old_header_id);
  auto old_header = reinterpret_cast<HashTableHeaderPage *>(old_head_page->GetData());
  for (size_t block_idx = 0; block_idx < old_header->NumBlocks(); block_idx++) {
    page_id_t block_id = old_header->GetBlockPageId(block_idx);
    Page* _block = buffer_pool_manager_->FetchPageImpl(block_id);
    HASH_TABLE_BLOCK_TYPE* block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(_block->GetData());
    for (slot_offset_t slot = 0; slot < BLOCK_ARRAY_SIZE; slot++) {
      if (block->IsReadable(slot)) {
        KeyType key = block->KeyAt(slot);
        ValueType value = block->ValueAt(slot);
        Insert(nullptr, key, value);
      }
    }
    buffer_pool_manager_->UnpinPageImpl(block_id, false);
    buffer_pool_manager_->DeletePageImpl(block_id);
  }
  buffer_pool_manager_->UnpinPageImpl(old_header_id, false);
  buffer_pool_manager_->DeletePageImpl(old_header_id);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  Page* _head = buffer_pool_manager_->FetchPageImpl(header_page_id_);
  _head->RLatch();
  auto head = reinterpret_cast<HashTableHeaderPage *>(_head->GetData());
  size_t size = head->GetSize();
  _head->RUnlatch();
  buffer_pool_manager_->UnpinPageImpl(header_page_id_, false);
  table_latch_.RUnlock();
  return size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
