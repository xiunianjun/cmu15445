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
INDEXITERATOR_TYPE::IndexIterator(INDEXITERATOR_TYPE &it) : bpm_(it.bpm_), pgid_(it.pgid_), cnt_(it.cnt_) {
  if (pgid_ != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageRead(pgid_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t pgid, int cnt) : bpm_(bpm), pgid_(pgid), cnt_(cnt) {
  if (pgid_ != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageRead(pgid_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t pgid) : bpm_(bpm), pgid_(pgid) {
  if (pgid_ != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageRead(pgid_);
  }
  cnt_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return pgid_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto page = guard_.As<LeafPage>();
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
  auto page = guard_.As<LeafPage>();
  cnt_++;
  if (cnt_ >= page->GetSize()) {
    pgid_ = page->GetNextPageId();
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
