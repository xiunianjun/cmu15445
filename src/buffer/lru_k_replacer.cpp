//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  if (cache_size_ == 0 && record_size_ == 0) {
    return false;
  }
  if (record_size_ != 0) {
    size_t min = 2147483647;
    // move from the record list
    auto remove_it = visit_record_.begin();
    for (auto it = visit_record_.begin(); it != visit_record_.end(); it++) {
      if (it->second.is_evictable_) {
        if (it->second.history_.front() < min) {
          remove_it = it;
          min = it->second.history_.front();
        }
      }
    }
    if (min == 2147483647) {
      return false;
    }
    *frame_id = remove_it->first;
    visit_record_.erase(remove_it);
    record_size_--;
    return true;
  }
  size_t min = 2147483647;
  auto remove_it = cache_data_.begin();
  for (auto it = cache_data_.begin(); it != cache_data_.end(); it++) {
    if (it->second.is_evictable_) {
      if (it->second.history_.front() < min) {
        remove_it = it;
        min = it->second.history_.front();
      }
    }
  }
  if (min == 2147483647) {
    return false;
  }
  *frame_id = remove_it->first;
  cache_data_.erase(remove_it);
  cache_size_--;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id > static_cast<int>(capacity_)) {
    throw Exception(fmt::format("RecordAccess:frame_id invalid."));
  }

  bool is_find = false;
  std::lock_guard<std::mutex> lck(latch_);
  auto it = cache_data_.find(frame_id);
  if (it != cache_data_.end()) {
    is_find = true;
    if (access_type != AccessType::Scan) {
      it->second.k_++;
      it->second.history_.push_back(current_timestamp_++);
      if (it->second.history_.size() > k_) {
        it->second.history_.pop_front();
      }
    }
  }
  if (!is_find) {
    auto it = visit_record_.find(frame_id);
    if (it != visit_record_.end()) {
      is_find = true;
      if (access_type != AccessType::Scan) {
        it->second.k_++;
        it->second.history_.push_back(current_timestamp_++);

        if (it->second.history_.size() >= k_) {
          LRUKNode node;
          node.history_ = std::list(it->second.history_);
          node.k_ = it->second.k_;
          node.fid_ = it->first;
          node.is_evictable_ = it->second.is_evictable_;

          if (it->second.is_evictable_) {
            cache_size_++;
            record_size_--;
          }
          cache_data_.emplace(node.fid_, node);
          visit_record_.erase(it);
        }
      }
    }
    if (!is_find) {
      // add to the visit record
      LRUKNode node;
      node.history_.push_back(current_timestamp_++);
      node.k_ = 1;
      node.fid_ = frame_id;
      node.is_evictable_ = true;
      visit_record_.emplace(node.fid_, node);
      record_size_++;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // if(frame_id > capacity_){
  if (frame_id > static_cast<int>(capacity_)) {
    throw Exception(fmt::format("SetEvictable:frame_id invalid."));
  }
  std::lock_guard<std::mutex> lck(latch_);
  auto it = cache_data_.find(frame_id);
  if (it != cache_data_.end()) {
    if (!it->second.is_evictable_ && set_evictable) {
      cache_size_++;
    } else if (it->second.is_evictable_ && !set_evictable) {
      cache_size_--;
    }
    it->second.is_evictable_ = set_evictable;
    return;
  }

  it = visit_record_.find(frame_id);
  if (it != visit_record_.end()) {
    if (!it->second.is_evictable_ && set_evictable) {
      record_size_++;
    } else if (it->second.is_evictable_ && !set_evictable) {
      record_size_--;
    }
    it->second.is_evictable_ = set_evictable;
    return;
  }
  // throw exception
  throw Exception(fmt::format("SetEvictable:can't find target frame."));
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // if(frame_id > capacity_){
  if (frame_id > static_cast<int>(capacity_)) {
    throw Exception(fmt::format("Remove:frame_id invalid."));
  }
  std::lock_guard<std::mutex> lck(latch_);
  auto it = cache_data_.find(frame_id);
  if (it != cache_data_.end()) {
    if (it->second.is_evictable_) {
      cache_size_--;
      cache_data_.erase(it);
      return;
    }
    throw Exception(fmt::format("Remove:target frame is not evictable."));
  }

  it = visit_record_.find(frame_id);
  if (it != visit_record_.end()) {
    if (it->second.is_evictable_) {
      record_size_--;
      visit_record_.erase(it);
      return;
    }
    throw Exception(fmt::format("Remove:target frame is not evictable."));
  }
}

// auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
