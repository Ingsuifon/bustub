//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    latch_.unlock();
    Page *page = pages_ + it->second;
    if (page->pin_count_++ == 0)
      replacer_->Pin(page_id);   
    return page;
  }
  frame_id_t targetFrame = -1;
  if (!free_list_.empty()) {
    targetFrame = free_list_.front();
    free_list_.pop_front();
    replacer_->Unpin(targetFrame);
  } else {
    replacer_->Victim(&targetFrame);
  }
  if (targetFrame == -1) {
    latch_.unlock();
    return nullptr;
  }
  replacer_->Pin(targetFrame);
  Page *target = pages_ + targetFrame;
  page_table_.erase(target->GetPageId());
  page_table_[page_id] = targetFrame;
  if (target->IsDirty())
    disk_manager_->WritePage(target->GetPageId(), target->GetData());
  disk_manager_->ReadPage(page_id, target->GetData());
  target->page_id_ = page_id;
  target->pin_count_ = 1;
  target->is_dirty_ = false;
  latch_.unlock();
  return target;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame = it->second;
    Page *page = pages_ + frame;
    page->is_dirty_ = page->is_dirty_ ? true : is_dirty;
    if (page->GetPinCount() <= 0) return false;
    if (--page->pin_count_ == 0) replacer_->Unpin(frame);
    return true;
  }
  return false; 
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame = it->second;
    Page *page = pages_ + frame;
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t targetFrame = -1;
  latch_.lock();
  if (!free_list_.empty()) {
    targetFrame = free_list_.front();
    free_list_.pop_front();
    replacer_->Unpin(targetFrame);
    replacer_->Pin(targetFrame);
    latch_.unlock();
  } else {
    latch_.unlock();
    replacer_->Victim(&targetFrame);
  }
  if (targetFrame == -1) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();
  Page *page = pages_ + targetFrame;
  if (page_table_.find(page->GetPageId()) != page_table_.end()) {
    page_table_.erase(page->GetPageId());
    if (page->IsDirty()) disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();
  page_table_[page->GetPageId()] = targetFrame;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  if (it == page_table_.end()) return true;
  Page *page = pages_ + it->second;
  if (page->GetPinCount() > 0) return false;
  page_table_.erase(page_id);
  disk_manager_->DeallocatePage(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  replacer_->Pin(it->second);
  latch_.lock();
  free_list_.emplace_front(it->second);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (Page *p = pages_; p < pages_ + pool_size_; p++)
    FlushPageImpl(p->GetPageId());
}

}  // namespace bustub
