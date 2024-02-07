//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void InsertTransactionSetByLockMode(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode)
  {
  case LockManager::LockMode::EXCLUSIVE:
    txn->GetExclusiveTableLockSet()->emplace(oid);
    break;
  case LockManager::LockMode::SHARED:
    txn->GetSharedTableLockSet()->emplace(oid);
    break;
  case LockManager::LockMode::INTENTION_EXCLUSIVE:
    txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
    break;
  case LockManager::LockMode::INTENTION_SHARED:
    txn->GetIntentionSharedTableLockSet()->emplace(oid);
    break;
  case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
    txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
    break;
  default:
    break;
  }
}

void EraseTransactionSetByLockMode(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode)
  {
  case LockManager::LockMode::EXCLUSIVE:
    txn->GetExclusiveTableLockSet()->erase(txn->GetExclusiveTableLockSet()->find(oid));
    break;
  case LockManager::LockMode::SHARED:
    txn->GetSharedTableLockSet()->erase(txn->GetSharedTableLockSet()->find(oid));
    break;
  case LockManager::LockMode::INTENTION_EXCLUSIVE:
    txn->GetIntentionExclusiveTableLockSet()->erase(txn->GetIntentionExclusiveTableLockSet()->find(oid));
    break;
  case LockManager::LockMode::INTENTION_SHARED:
    txn->GetIntentionSharedTableLockSet()->erase(txn->GetIntentionSharedTableLockSet()->find(oid));
    break;
  case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(txn->GetSharedIntentionExclusiveTableLockSet()->find(oid));
    break;
  default:
    break;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /* validate legality of lock mode and transaction state */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // S/IS/SIX locks are not required under READ_UNCOMMITTED
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    // No locks are allowed in the SHRINKING state in REPEATABLE_READ level
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }

    // X/IX locks on rows are not allowed if the the Transaction State is SHRINKING
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
    it = table_lock_map_.find(oid);
  }
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> req_queue_lock(it->second->latch_);

  bool is_upgraded = false;

  /* case of upgrade */
  for (auto req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
    if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
      if (it->second->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      LockMode prev_lock_mode = (*req_it)->lock_mode_;
      if (prev_lock_mode == lock_mode) {
        return true;
      }

      if ((prev_lock_mode == LockMode::EXCLUSIVE) ||
        (prev_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) ||
        (prev_lock_mode == LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) ||
        (prev_lock_mode == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)
      ) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // do an upgrade
      it->second->upgrading_ = txn->GetTransactionId();
      delete (*req_it);
      it->second->request_queue_.erase(req_it);

      EraseTransactionSetByLockMode(txn, prev_lock_mode, oid);

      is_upgraded = true;
      break;
    }
  }

  if (!is_upgraded) {
    it->second->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }
  auto req_it = std::prev(it->second->request_queue_.end());
  if (is_upgraded) {
    req_it = it->second->request_queue_.end();
  }

  switch (lock_mode)
  {
  case LockMode::EXCLUSIVE:
    while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) 
            || (!(std::all_of(it->second->request_queue_.begin(), req_it, [is_upgraded](LockRequest* req) {
            return (is_upgraded && (req->granted_ == false)); })) // have another kind of lock
            && txn->GetState() != TransactionState::ABORTED)) {
      it->second->cv_.wait(req_queue_lock);
    }
    break;
  case LockMode::SHARED:
    while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) || ((!(std::all_of(it->second->request_queue_.begin(), req_it, [is_upgraded](LockRequest* req) {
        return (is_upgraded && (req->granted_ == false)) || req->lock_mode_ == LockMode::SHARED || req->lock_mode_ == LockMode::INTENTION_SHARED; }))) // have another kind of lock
        && txn->GetState() != TransactionState::ABORTED)) {
      it->second->cv_.wait(req_queue_lock);
    }
    break;
  case LockMode::INTENTION_EXCLUSIVE:
    while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) || ((!(std::all_of(it->second->request_queue_.begin(), req_it, [is_upgraded](LockRequest* req) {
        return (is_upgraded && (req->granted_ == false)) || req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || req->lock_mode_ == LockMode::INTENTION_SHARED; }))) // have another kind of lock
        && txn->GetState() != TransactionState::ABORTED)) {
      it->second->cv_.wait(req_queue_lock);
    }
    break;
  case LockMode::INTENTION_SHARED:
    while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) || ((!(std::all_of(it->second->request_queue_.begin(), req_it, [is_upgraded](LockRequest* req) {
        return (is_upgraded && (req->granted_ == false)) || req->lock_mode_ != LockMode::EXCLUSIVE; }))) // have another kind of lock
        && txn->GetState() != TransactionState::ABORTED)) {
      it->second->cv_.wait(req_queue_lock);
    }
    break;
  case LockMode::SHARED_INTENTION_EXCLUSIVE:
    while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) || ((!(std::all_of(it->second->request_queue_.begin(), req_it, [is_upgraded](LockRequest* req) {
        return (is_upgraded && (req->granted_ == false)) || req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE; }))) // have another kind of lock
        && txn->GetState() != TransactionState::ABORTED)) {
      it->second->cv_.wait(req_queue_lock);
    }
    break;
  default:
    break;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (is_upgraded) {
    auto upgraded_req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    upgraded_req->granted_ = true;
    it->second->request_queue_.push_front(upgraded_req);
    it->second->upgrading_ = INVALID_TXN_ID;
  } else {
    (*req_it)->granted_ = true;
  }

  InsertTransactionSetByLockMode(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  txn->SetState(TransactionState::SHRINKING);

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> req_queue_lock(it->second->latch_);
  if (it->second->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  LockMode lock_mode;

  for (auto req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ) {
    if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT((*req_it)->granted_, "Must have been granted in unlock function!");
      lock_mode = (*req_it)->lock_mode_;
      delete (*req_it);
      req_it = it->second->request_queue_.erase(req_it);
      break;
    } else {
      ++req_it;
    }
  }

  EraseTransactionSetByLockMode(txn, lock_mode, oid);
  // printf("txn id %d table %d notify!!\n", txn->GetTransactionId(), oid);
  it->second->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
