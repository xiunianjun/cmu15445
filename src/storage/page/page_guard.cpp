#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

/** TODO(P1): Add implementation
 *
 * @brief Move constructor for BasicPageGuard
 *
 * When you call BasicPageGuard(std::move(other_guard)), you
 * expect that the new guard will behave exactly like the other
 * one. In addition, the old page guard should not be usable. For
 * example, it should not be possible to call .Drop() on both page
 * guards and have the pin count decrease by 2.
 */
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  page_ = that.page_;
  that.page_ = nullptr;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;
}

/** TODO(P1): Add implementation
 *
 * @brief Drop a page guard
 *
 * Dropping a page guard should clear all contents
 * (so that the page guard is no longer useful), and
 * it should tell the BPM that we are done using this page,
 * per the specification in the writeup.
 */
void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
  }
}

/** TODO(P1): Add implementation
 *
 * @brief Move assignment for BasicPageGuard
 *
 * Similar to a move constructor, except that the move
 * assignment assumes that BasicPageGuard already has a page
 * being guarded.
 * Think carefully about what should happen when
 * a guard replaces its held page with a different one, given
 * the purpose of a page guard.
 */
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  page_ = that.page_;
  that.page_ = nullptr;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

/** TODO(P1): Add implementation
 *
 * @brief Move constructor for ReadPageGuard
 *
 * Very similar to BasicPageGuard. You want to create
 * a ReadPageGuard using another ReadPageGuard. Think
 * about if there's any way you can make this easier for yourself...
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = BasicPageGuard(std::move(that.guard_));
  that.guard_.page_ = nullptr;
}

/** TODO(P1): Add implementation
 *
 * @brief Move assignment for ReadPageGuard
 *
 * Very similar to BasicPageGuard. Given another ReadPageGuard,
 * replace the contents of this one with that one.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  that.guard_.page_ = nullptr;
  return *this;
}

/** TODO(P1): Add implementation
 *
 * @brief Drop a ReadPageGuard
 *
 * ReadPageGuard's Drop should behave similarly to BasicPageGuard,
 * except that ReadPageGuard has an additional resource - the latch!
 * However, you should think VERY carefully about in which order you
 * want to release these resources.
 */
void ReadPageGuard::Drop() {
  auto page = guard_.page_;
  if (page == nullptr) {
    return;
  }

  guard_.Drop();
  page->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = BasicPageGuard(std::move(that.guard_));
  that.guard_.page_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  that.guard_.page_ = nullptr;
  return *this;
}

void WritePageGuard::Drop() {
  auto page = guard_.page_;
  if (page == nullptr) {
    return;
  }

  guard_.Drop();
  page->WUnlatch();
}

WritePageGuard::~WritePageGuard() { Drop(); }
}  // namespace bustub
