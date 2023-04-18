//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Init(int max_size) {
    SetMaxSize(max_size);
    SetSize(1); // internal page默认有一个空子节点
    SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::KeyAt(int index) const -> KeyType {
  if(index >= 1 && index < GetSize()){ // key从1开始遍历
      return array_[index].first;
  }
  return {};
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key) {
  if(index >= 1 && index < GetSize()){
      array_[index].first = std::move(key);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  if(index >= 0 && index < GetSize()){
      array_[index].second = std::move(value);
  }
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const -> ValueType {
  if(index >= 0 && index < GetSize()){
      return array_[index].second;
  }
  return {};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ValueIndex(const ValueType &value) const -> int {
  for(int i=0;i<GetSize();i++){
      if(array_[i].second == value){
	      return i;
      }	
  }
  return -1;// 随便return了个不合理值
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
