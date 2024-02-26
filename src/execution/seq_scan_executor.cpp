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
  try {
    if (txn_->GetExclusiveTableLockSet()->find(plan_->GetTableOid()) == txn_->GetExclusiveTableLockSet()->end()) {
      bool lock_success = true;
      if (exec_ctx_->IsDelete()) {
        lock_success = exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                              plan_->GetTableOid());
      } else {
        // is not read uncommitted and do not have a X lock
        if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          lock_success = exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED,
                                                                plan_->GetTableOid());
        }
      }

      if (!lock_success) {
        throw ExecutionException("fail to get table lock in the seq-scan.");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("fail to get table lock in the seq-scan.");
  }

  table_iterator_ = std::make_unique<TableIterator>(
      // exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
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
    try {
      if (txn_->GetExclusiveRowLockSet()->find(plan_->GetTableOid()) == txn_->GetExclusiveRowLockSet()->end()) {
        bool lock_success = true;
        if (exec_ctx_->IsDelete()) {
          lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE,
                                                              plan_->GetTableOid(), table_iterator_->GetRID());
          locked = true;
        } else {
          // is not read uncommitted and do not have a X lock
          if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED,
                                                                plan_->GetTableOid(), table_iterator_->GetRID());
            locked = true;
          }
        }

        if (!lock_success) {
          throw ExecutionException("fail to get table lock in the seq-scan.");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("fail to get row lock in the seq-scan.");
    }

    auto tuple_pair = table_iterator_->GetTuple();
    tuple_meta = tuple_pair.first;
    tuple = tuple_pair.second;
    rid = table_iterator_->GetRID();
    ++(*table_iterator_);

    if (tuple_meta.is_deleted_ || (plan_->filter_predicate_ != nullptr &&
                                   !(plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema()).GetAs<bool>()))) {
      if (locked) {
        try {
          exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->GetTableOid(), rid, true);
        } catch (TransactionAbortException &e) {
          throw ExecutionException("fail to release row lock in the seq-scan.");
        }
      }
      continue;
    }

    if (locked && !(exec_ctx_->IsDelete()) && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        exec_ctx_->GetLockManager()->UnlockRow(txn_, plan_->GetTableOid(), rid, false);
      } catch (TransactionAbortException &e) {
        throw ExecutionException("fail to release row lock in the seq-scan in READ_COMMITTED level.");
      }
    }
    break;
  }

  *param_tuple = tuple;
  *param_rid = rid;
  return EXECUTOR_ACTIVE;
}

}  // namespace bustub
