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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  current_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta tuple_meta;
  do {
    if (current_iterator_->IsEnd()) {
      return false;
    }

    auto table_heap = table_info_->table_.get();

    *rid = (**current_iterator_).second;
    auto tuple_pair = table_heap->GetTuple(*rid);
    *tuple = tuple_pair.second;
    tuple_meta = tuple_pair.first;

    ++(*current_iterator_);
  } while (tuple_meta.is_deleted_);

  return true;
}

}  // namespace bustub
