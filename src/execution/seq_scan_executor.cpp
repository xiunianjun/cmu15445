//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  if (exec_ctx_->IsDelete()) {
    exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->GetTableOid());
  } else {
    if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    }
  }

  table_iterator_ = std::make_unique<TableIterator>(
      exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  TupleMeta tuple_meta;
  Tuple tuple;
  RID rid;

  while (true) {
    if (table_iterator_->IsEnd()) {
      return EXECUTOR_EXHAUSTED;
    }

    bool locked = false;
    if (exec_ctx_->IsDelete()) {
      exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(),
                                           table_iterator_->GetRID());
      locked = true;
    } else {
      if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, plan_->GetTableOid(),
                                             table_iterator_->GetRID());
        locked = true;
      }
    }

    auto tuple_pair = table_iterator_->GetTuple();
    tuple_meta = tuple_pair.first;
    tuple = tuple_pair.second;
    rid = table_iterator_->GetRID();
    ++(*table_iterator_);

    if (tuple_meta.is_deleted_ || (plan_->filter_predicate_ != nullptr &&
                                   !(plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema()).GetAs<bool>()))) {
      if (locked) {
        exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->GetTableOid(), rid, true);
      }
      continue;
    }

    if (!(exec_ctx_->IsDelete()) && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->GetTableOid(), rid, false);
    }
    break;
  }

  *param_tuple = tuple;
  *param_rid = rid;
  return EXECUTOR_ACTIVE;
}

}  // namespace bustub
