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
  table_oid_ = table_info_->oid_;
  txn_ = exec_ctx_->GetTransaction();

  bool lock_success = true;
  try {
    if (exec_ctx_->IsDelete()) {
      lock_success =
          exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid_);
    } else {
      // is not read uncommitted and do not have a X lock
      if (txn_->GetIntentionExclusiveTableLockSet()->find(table_oid_) ==
          txn_->GetIntentionExclusiveTableLockSet()->end()) {
        if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          lock_success =
              exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, table_oid_);
        }
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("fail to get table lock in the seq-scan.");
  }
  if (!lock_success) {
    throw ExecutionException("fail to get table lock in the seq-scan.");
  }

  if (plan_->first_key_.has_value()) {
    current_iterator_ =
        std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator(plan_->first_key_.value()));

  } else {
    current_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator());
  }

  if (plan_->last_key_.has_value()) {
    end_iterator_ =
        std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetBeginIterator(plan_->last_key_.value()));
    if (!(end_iterator_->IsEnd())) {
      ++(*end_iterator_);
    }
  } else {
    end_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree->GetEndIterator());
  }
}

auto IndexScanExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  TupleMeta tuple_meta;
  Tuple tuple;
  RID rid;
  while (true) {
    if (current_iterator_->IsEnd() || *current_iterator_ == *end_iterator_) {
      return false;
    }

    bool locked = false;
    rid = (**current_iterator_).second;
    try {
      if (txn_->GetExclusiveRowLockSet()->find(table_oid_) == txn_->GetExclusiveRowLockSet()->end()) {
        bool lock_success = true;
        if (exec_ctx_->IsDelete()) {
          lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, table_oid_, rid);
          locked = true;
        } else {
          // is not read uncommitted and do not have a X lock
          if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            lock_success = exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, table_oid_, rid);
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

    auto table_heap = table_info_->table_.get();
    auto tuple_pair = table_heap->GetTuple(rid);
    tuple = tuple_pair.second;
    tuple_meta = tuple_pair.first;

    ++(*current_iterator_);

    if ((tuple_meta.is_deleted_ ||
         (plan_->Predicate() != nullptr && !(plan_->Predicate()->Evaluate(&tuple, GetOutputSchema()).GetAs<bool>())))) {
      if (locked) {
        try {
          exec_ctx_->GetLockManager()->UnlockRow(txn_, table_oid_, rid, true);
        } catch (TransactionAbortException &e) {
          throw ExecutionException("fail to release row lock in the seq-scan.");
        }
      }
      continue;
    }

    if (locked && !(exec_ctx_->IsDelete()) && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        exec_ctx_->GetLockManager()->UnlockRow(txn_, table_oid_, rid, false);
      } catch (TransactionAbortException &e) {
        throw ExecutionException("fail to release row lock in the seq-scan in READ_COMMITTED level.");
      }
    }
    break;
  }

  *param_tuple = tuple;
  *param_rid = rid;
  return true;
}

}  // namespace bustub
