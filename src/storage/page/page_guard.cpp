#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  valid_ = that.valid_;
  page_ = that.page_;
  is_unpin_ = that.is_unpin_;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;
  that.valid_ = false;
}

void BasicPageGuard::Drop() {
  if (!valid_) {
    return;
  }
  if (!is_unpin_ && bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    is_unpin_ = true;
    // bpm_->DeletePage(page_->GetPageId());
  }
  valid_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  that.is_unpin_ = true;
  valid_ = that.valid_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  Drop();
  // delete page_;
  // delete bpm_;
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (that.is_locked_) {
    that.guard_.page_->RUnlatch();
    that.is_locked_ = false;
  }
  guard_ = BasicPageGuard(std::move(that.guard_));
  guard_.page_->RLatch();
  is_locked_ = true;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_.Drop();
  guard_.is_unpin_ = that.guard_.is_unpin_;
  that.guard_.is_unpin_ = true;
  guard_.valid_ = that.guard_.valid_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.bpm_ = that.guard_.bpm_;
  if (that.is_locked_) {
    that.guard_.page_->RUnlatch();
    that.is_locked_ = false;
  }

  guard_.page_ = that.guard_.page_;
  guard_.page_->RLatch();
  is_locked_ = true;
  return *this;
}

void ReadPageGuard::Drop() {
  if (is_locked_ && guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    is_locked_ = false;
  }

  // if (!guard_.valid_) {
  //   return;
  // }

  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
  // guard_.~BasicPageGuard();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (that.is_locked_) {
    that.guard_.page_->WUnlatch();
    that.is_locked_ = false;
  }
  guard_ = BasicPageGuard(std::move(that.guard_));
  guard_.page_->WLatch();
  is_locked_ = true;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_.Drop();
  guard_.is_unpin_ = that.guard_.is_unpin_;
  that.guard_.is_unpin_ = true;
  guard_.valid_ = that.guard_.valid_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.bpm_ = that.guard_.bpm_;
  if (that.is_locked_) {
    that.guard_.page_->WUnlatch();
    that.is_locked_ = false;
  }

  guard_.page_ = that.guard_.page_;
  guard_.page_->WLatch();
  is_locked_ = true;
  return *this;
}

void WritePageGuard::Drop() {
  if (is_locked_ && guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    is_locked_ = false;
  }

  // if (!guard_.valid_) {
  //   return;
  // }

  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
  // guard_.~BasicPageGuard();
}
}  // namespace bustub
