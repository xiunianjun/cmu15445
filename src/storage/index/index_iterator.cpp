/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(INDEXITERATOR_TYPE &it) : bpm_(it.bpm_), pgid_(it.pgid_), cnt_(it.cnt_) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(INDEXITERATOR_TYPE &&it) noexcept : bpm_(it.bpm_), pgid_(it.pgid_), cnt_(it.cnt_) {
  // Set the source iterator's pgid to INVALID_PAGE_ID to indicate it's moved-from state
  it.pgid_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t pgid, int cnt)
    : bpm_(bpm), pgid_(pgid), cnt_(cnt) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t pgid) : bpm_(bpm), pgid_(pgid) { cnt_ = 0; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return pgid_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(!(this->IsEnd()), "Can't apply * operator to end iterator!");
  auto guard = bpm_->FetchPageRead(pgid_);
  auto page = guard.As<LeafPage>();
  BUSTUB_ASSERT(cnt_ < page->GetSize(), "Can't apply * operator to end iterator!");
  return page->PairAt(cnt_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return ((pgid_ == itr.pgid_) && (cnt_ == itr.cnt_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !((pgid_ == itr.pgid_) && (cnt_ == itr.cnt_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (pgid_ == INVALID_PAGE_ID) {
    return *this;
  }

  auto guard = bpm_->FetchPageRead(pgid_);
  auto page = guard.As<LeafPage>();
  cnt_++;
  BUSTUB_ASSERT(page->GetSize() > 0, "page size must be bigger than zero!");
  if (cnt_ >= page->GetSize()) {
    pgid_ = page->GetNextPageId();
    if (pgid_ != INVALID_PAGE_ID) {
      guard = bpm_->FetchPageRead(pgid_);
    }
    cnt_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
