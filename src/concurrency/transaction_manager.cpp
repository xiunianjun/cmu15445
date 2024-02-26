//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  for (auto it = txn->GetWriteSet()->rbegin(); it != txn->GetWriteSet()->rend(); ++it) {
    auto meta = it->table_heap_->GetTupleMeta(it->rid_);
    meta.is_deleted_ = !(meta.is_deleted_);
    it->table_heap_->UpdateTupleMeta(meta, it->rid_);
  }

  for (auto it = txn->GetIndexWriteSet()->rbegin(); it != txn->GetIndexWriteSet()->rend(); ++it) {
    auto index_info = it->catalog_->GetIndex(it->index_oid_);
    if (it->wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(
          it->tuple_.KeyFromTuple(it->catalog_->GetTable(it->table_oid_)->schema_, index_info->key_schema_,
                                  index_info->index_->GetKeyAttrs()),
          it->rid_, txn);
    } else {
      index_info->index_->DeleteEntry(
          it->tuple_.KeyFromTuple(it->catalog_->GetTable(it->table_oid_)->schema_, index_info->key_schema_,
                                  index_info->index_->GetKeyAttrs()),
          it->rid_, txn);
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
