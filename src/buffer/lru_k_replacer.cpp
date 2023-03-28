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
    // move from the record list
    for (auto it = visit_record_.begin(); it != visit_record_.end(); it++) {
      if (it->is_evictable_) {
        *frame_id = it->fid_;
        visit_record_.erase(it);
        record_size_--;
        return true;
      }
    }
  }
  for (auto it = cache_data_.begin(); it != cache_data_.end(); it++) {
    if (it->is_evictable_) {
      *frame_id = it->fid_;
      cache_data_.erase(it);
      cache_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id > static_cast<int>(capacity_)) {
    throw Exception(fmt::format("RecordAccess:frame_id invalid."));
  }
  bool is_find = false;
  std::lock_guard<std::mutex> lck(latch_);
  for (auto it = cache_data_.begin(); it != cache_data_.end(); it++) {
    if (it->fid_ == frame_id) {
      is_find = true;
      it->k_++;
      it->history_ = current_timestamp_;
      // it->history_.push_front(current_timestamp_);
      LRUKNode node;
      node.history_ = current_timestamp_;
      // node.history_.push_back(current_timestamp_);
      node.k_ = it->k_;
      node.fid_ = it->fid_;
      node.is_evictable_ = it->is_evictable_;

      cache_data_.emplace_back(node);
      // cache_data_.push_back(LRUKNode(*it));
      cache_data_.erase(it);
      break;
    }
  }
  if (!is_find) {
    for (auto it = visit_record_.begin(); it != visit_record_.end(); it++) {
      if (it->fid_ == frame_id) {
        is_find = true;
        it->k_++;
        it->history_ = current_timestamp_;
        if (it->k_ >= k_) {
          // move to the cache
          if (it->is_evictable_) {
            cache_size_++;
            record_size_--;
          }
          // cache_data_.push_back(LRUKNode(*it));
          LRUKNode node;
          node.history_ = current_timestamp_;
          // node.history_.push_back(current_timestamp_);
          node.k_ = it->k_;
          node.fid_ = it->fid_;
          node.is_evictable_ = it->is_evictable_;

          cache_data_.emplace_back(node);
        } else {
          // keep in the record
          LRUKNode node;
          node.history_ = current_timestamp_;
          // node.history_.push_back(current_timestamp_);
          node.k_ = it->k_;
          node.fid_ = it->fid_;
          node.is_evictable_ = it->is_evictable_;

          visit_record_.emplace_back(node);
          // visit_record_.push_back(*it);
        }
        cache_data_.erase(it);
        break;
      }
    }
    if (!is_find) {
      // add to the visit record
      LRUKNode node;
      node.history_ = current_timestamp_;
      // node.history_.push_back(current_timestamp_);
      node.k_ = 1;
      node.fid_ = frame_id;
      node.is_evictable_ = true;
      visit_record_.push_back(node);
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
  for (auto &it : cache_data_) {
    if (it.fid_ == frame_id) {
      if (!it.is_evictable_ && set_evictable) {
        cache_size_++;
      } else if (it.is_evictable_ && !set_evictable) {
        cache_size_--;
      }
      it.is_evictable_ = set_evictable;
      return;
    }
  }
  // for (auto it = visit_record_.begin(); it != visit_record_.end(); it++) {
  for (auto &it : visit_record_) {
    if (it.fid_ == frame_id) {
      if (!it.is_evictable_ && set_evictable) {
        record_size_++;
      } else if (it.is_evictable_ && !set_evictable) {
        record_size_--;
      }
      it.is_evictable_ = set_evictable;
      return;
    }
  }

  // throw exception
  throw Exception(fmt::format("SetEvictable:can't find target frame."));
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  // if(frame_id > capacity_){
  if (frame_id > static_cast<int>(capacity_)) {
    throw Exception(fmt::format("Remove:frame_id invalid."));
  }
  for (auto it = cache_data_.begin(); it != cache_data_.end(); it++) {
    if (it->fid_ == frame_id) {
      if (it->is_evictable_) {
        cache_size_--;
      } else {
        throw Exception(fmt::format("Remove:target frame is not evictable."));
      }
      cache_data_.erase(it);
      return;
    }
  }
  for (auto it = visit_record_.begin(); it != visit_record_.end(); it++) {
    if (it->fid_ == frame_id) {
      if (it->is_evictable_) {
        record_size_--;
      } else {
        throw Exception(fmt::format("Remove:target frame is not evictable."));
      }
      visit_record_.erase(it);
      return;
    }
  }
}

// auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
