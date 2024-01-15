//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), is_end_(false) {
    }

void IndexScanExecutor::Init() {
  is_end_ = false;
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);

  auto iterator = tree_->GetBeginIterator();
  if (!(iterator.IsEnd())) {
    next_key_ = (*iterator).first;
  }
  else {
    is_end_ = true;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  auto iterator = tree_->GetBeginIterator(next_key_);
  auto table_heap = table_info_->table_.get();

  *rid = (*iterator).second;
  *tuple = table_heap->GetTuple(*rid).second;

  ++iterator;
  if (!(iterator.IsEnd()))
    next_key_ = (*iterator).first;
  else
    is_end_ = true;

  return true;
}

}  // namespace bustub
