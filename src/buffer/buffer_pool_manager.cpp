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
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// 我们应该需要做:
// 如果replacer已满(freelist.empty())，则先调用其Evict方法；如果调用完后还是满，就return nullptr
// 记得调用完Evict后要把frame_id存入freelist,并且检查下其对应Page是否dirty
// 得到一个free的Page对象，填写其信息，包括调用AllocatePage获取page_id、从freelist中取号并放入replacer、初始化其data域
// 记得call setevictable来pin，和recoradaccess
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lck(latch_);
  if (free_list_.empty()) {
    frame_id_t fid;
    if (!replacer_->Evict(&fid)) {
      // 是否要设置为该值还存疑
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
    page_table_.erase(page_table_.find(pages_[fid].GetPageId()));
    pages_[fid].SetPageId(INVALID_PAGE_ID);
    // replacer_->SetEvictable(fid, true);

    free_list_.push_back(fid);
  }
  frame_id_t fid = free_list_.front();
  free_list_.pop_front();

  Page *res = &(pages_[fid]);
  *page_id = AllocatePage();
  res->SetPageId(*page_id);
  res->ResetMemory();

  page_table_.insert(std::make_pair(*page_id, fid));
  res->Pin();
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  res->SetDirty(false);

  return res;
}

// 首先查找page_table，在的话直接返回
// 然后再找free_list,free_list没有就调用evict,还是没有的话就返回nullptr.记得old dirty写入
// 调用newpage
// 从diskmanager把数据读进来。old dirty写入
// 跟newpage一样都要记得调用replacer的方法
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[fid].GetData());
    }
    // disk_manager_->ReadPage(page_id, pages_[fid].GetData());
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    return &(pages_[fid]);
  }
  if (free_list_.empty()) {
    frame_id_t fid;
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    if (pages_[fid].IsDirty()) {
      disk_manager_->WritePage(pages_[fid].GetPageId(), pages_[fid].GetData());
    }
    page_table_.erase(page_table_.find(pages_[fid].GetPageId()));
    pages_[fid].SetPageId(INVALID_PAGE_ID);
    // replacer_->SetEvictable(fid, true);
    free_list_.push_back(fid);
  }
  frame_id_t fid = free_list_.front();
  free_list_.pop_front();

  Page *res = &(pages_[fid]);
  res->SetPageId(page_id);

  page_table_.insert(std::make_pair(page_id, fid));
  disk_manager_->ReadPage(page_id, pages_[fid].GetData());
  res->Pin();
  res->SetDirty(false);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return res;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t fid = it->second;
  if (pages_[fid].GetPinCount() == 0) {
    return false;
  }
  pages_[fid].UnPin();
  pages_[fid].SetDirty(is_dirty);
  if (pages_[fid].GetPinCount() == 0) {
    replacer_->SetEvictable(fid, true);
  }
  return true;
}

/*
/   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
/   * Unset the dirty flag of the page after flushing.
*/
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t fid = it->second;
  disk_manager_->WritePage(page_id, pages_[fid].GetData());
  pages_[fid].SetDirty(false);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPage(pages_[i].GetPageId());
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t fid = it->second;
  if (pages_[fid].GetPinCount() != 0) {
    return false;
  }
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[fid].GetData());
  }
  page_table_.erase(it);
  pages_[fid].SetPageId(INVALID_PAGE_ID);
  pages_[fid].ResetMemory();
  replacer_->SetEvictable(fid, true);
  free_list_.push_back(fid);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return BasicPageGuard(this, FetchPage(page_id));
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  return ReadPageGuard(this, FetchPage(page_id));
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  return WritePageGuard(this, FetchPage(page_id));
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return BasicPageGuard(this, NewPage(page_id));
}

}  // namespace bustub
