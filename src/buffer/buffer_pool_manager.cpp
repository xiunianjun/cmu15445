//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager), pages_latch_(pool_size) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();

  if (free_list_.empty()) {  // has no free page containers, evict one
    frame_id_t fid;
    if (!replacer_->Evict(&fid)) {  // has no way to evict
      *page_id = INVALID_PAGE_ID;
      latch_.unlock();
      return nullptr;
    }

    page_table_.erase(page_table_.find(pages_[fid].GetPageId()));
    *page_id = AllocatePage();
    page_table_.insert(std::make_pair(*page_id, fid));

    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    pages_latch_[fid].lock();

    if (pages_[fid].IsDirty()) {  // write origin back
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
    latch_.unlock();

    Page *res = &(pages_[fid]);
    res->page_id_ = *page_id;
    res->ResetMemory();
    res->is_dirty_ = false;
    res->pin_count_ = 1;

    pages_latch_[fid].unlock();
    return res;
  }

  frame_id_t fid = free_list_.front();
  free_list_.pop_front();
  *page_id = AllocatePage();

  pages_latch_[fid].lock();
  page_table_.insert(std::make_pair(*page_id, fid));
  latch_.unlock();

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  Page *res = &(pages_[fid]);

  res->page_id_ = *page_id;
  res->ResetMemory();
  res->is_dirty_ = false;

  res->pin_count_ = 1;
  pages_latch_[fid].unlock();

  return res;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPage(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
 *
 * @param page_id id of page to be fetched
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;

    pages_latch_[fid].lock();
    replacer_->RecordAccess(fid, access_type);

    replacer_->SetEvictable(fid, false);

    pages_[fid].pin_count_++;
    pages_latch_[fid].unlock();

    latch_.unlock();
    return &(pages_[fid]);
  }
  
  if (free_list_.empty()) {
    frame_id_t fid;
    if (!replacer_->Evict(&fid)) {
      latch_.unlock();
      return nullptr;
    }
    
    page_table_.erase(page_table_.find(pages_[fid].GetPageId()));
    page_table_.insert(std::make_pair(page_id, fid));

    replacer_->RecordAccess(fid, access_type);
    replacer_->SetEvictable(fid, false);

    pages_latch_[fid].lock();

    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }

    Page *res = &(pages_[fid]);
    res->ResetMemory();
    // read from disk
    disk_manager_->ReadPage(page_id, res->GetData());
    latch_.unlock();

    res->is_dirty_ = false;
    res->page_id_ = page_id;
    res->pin_count_ = 1;

    pages_latch_[fid].unlock();
    return res;
  }
  frame_id_t fid = free_list_.front();
  free_list_.pop_front();
  pages_latch_[fid].lock();
  page_table_.insert(std::make_pair(page_id, fid));

  replacer_->RecordAccess(fid, access_type);

  replacer_->SetEvictable(fid, false);

  Page *res = &(pages_[fid]);

  res->page_id_ = page_id;
  res->ResetMemory();

  // read from disk
  disk_manager_->ReadPage(page_id, res->GetData());
  res->is_dirty_ = false;

  res->pin_count_ = 1;
  pages_latch_[fid].unlock();

  latch_.unlock();
  return res;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock_shared();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock_shared();
    return false;
  }

  frame_id_t fid = it->second;
  pages_latch_[fid].lock();
  if (is_dirty) {
    pages_[fid].is_dirty_ = is_dirty;
  }

  if (pages_[fid].GetPinCount() == 0) {
    latch_.unlock_shared();
    pages_latch_[fid].unlock();
    return false;
  }
  pages_[fid].pin_count_--;

  if (pages_[fid].GetPinCount() == 0) {
    replacer_->SetEvictable(fid, true);  // can only be evict when has no pin count
  }
  pages_latch_[fid].unlock();

  latch_.unlock_shared();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  latch_.lock_shared();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock_shared();
    return false;
  }
  frame_id_t fid = it->second;

  pages_latch_[fid].lock();

  if (pages_[fid].is_dirty_)
    disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;

  pages_latch_[fid].unlock();

  latch_.unlock_shared();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPage(pages_[i].GetPageId());
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  frame_id_t fid = it->second;
  pages_latch_[fid].lock();

  if (pages_[fid].GetPinCount() != 0) {
    pages_latch_[fid].unlock();
    latch_.unlock();
    return false;
  }

  page_table_.erase(it);
  free_list_.push_back(fid);
  replacer_->SetEvictable(fid, true);
  // latch_.unlock();
  // latch_.lock_shared();
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[fid].GetData());
  }
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].ResetMemory();
  pages_[fid].is_dirty_ = false;
  pages_[fid].pin_count_ = 0;

  pages_latch_[fid].unlock();

  DeallocatePage(page_id);

  latch_.unlock();
  // latch_.unlock_shared();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
