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

void InsertTransactionTableSetByLockMode(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
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

void InsertTransactionRowSetByLockMode(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid,
                                       const RID &rid) {
  auto it = txn->GetExclusiveRowLockSet()->find(oid);
  switch (lock_mode) {
    case LockManager::LockMode::EXCLUSIVE:
      it = txn->GetExclusiveRowLockSet()->find(oid);
      if (it == txn->GetExclusiveRowLockSet()->end()) {
        txn->GetExclusiveRowLockSet()->emplace(oid, std::unordered_set<bustub::RID>());
        it = txn->GetExclusiveRowLockSet()->find(oid);
      }
      it->second.emplace(rid);
      break;
    case LockManager::LockMode::SHARED:
      it = txn->GetSharedRowLockSet()->find(oid);
      if (it == txn->GetSharedRowLockSet()->end()) {
        txn->GetSharedRowLockSet()->emplace(oid, std::unordered_set<bustub::RID>());
        it = txn->GetSharedRowLockSet()->find(oid);
      }
      it->second.emplace(rid);
      break;
    default:
      break;
  }
}

void EraseTransactionTableSetByLockMode(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
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

void EraseTransactionRowSetByLockMode(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid,
                                      const RID &rid) {
  auto it = txn->GetExclusiveRowLockSet()->find(oid);
  switch (lock_mode) {
    case LockManager::LockMode::EXCLUSIVE:
      it = txn->GetExclusiveRowLockSet()->find(oid);
      it->second.erase(it->second.find(rid));
      if (it->second.empty()) {
        txn->GetExclusiveRowLockSet()->erase(it);
      }
      break;
    case LockManager::LockMode::SHARED:
      it = txn->GetSharedRowLockSet()->find(oid);
      it->second.erase(it->second.find(rid));
      if (it->second.empty()) {
        txn->GetSharedRowLockSet()->erase(it);
      }
      break;
    default:
      break;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /* validate legality of lock mode and transaction state */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // S/IS/SIX locks are not required under READ_UNCOMMITTED
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
        lock_mode == LockMode::INTENTION_SHARED) {
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
          (prev_lock_mode == LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE &&
           lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) ||
          (prev_lock_mode == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
           lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // do an upgrade
      it->second->upgrading_ = txn->GetTransactionId();
      delete (*req_it);
      it->second->request_queue_.erase(req_it);

      EraseTransactionTableSetByLockMode(txn, prev_lock_mode, oid);

      is_upgraded = true;
      break;
    }
  }

  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             (!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                            [is_upgraded](LockRequest *req) {
                              return (is_upgraded && (!(req->granted_)));
                            }))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    case LockMode::SHARED:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             ((!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) || req->lock_mode_ == LockMode::SHARED ||
                                      req->lock_mode_ == LockMode::INTENTION_SHARED;
                             })))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             ((!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) ||
                                      req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                                      req->lock_mode_ == LockMode::INTENTION_SHARED;
                             })))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    case LockMode::INTENTION_SHARED:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             ((!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) || req->lock_mode_ != LockMode::EXCLUSIVE;
                             })))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             ((!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) ||
                                      req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE;
                             })))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    default:
      break;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    it->second->cv_.notify_all();
    return false;
  }

  auto new_req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  new_req->granted_ = true;
  if (is_upgraded) {
    it->second->request_queue_.push_front(new_req);
    it->second->upgrading_ = INVALID_TXN_ID;
  } else {
    it->second->request_queue_.push_back(new_req);
  }

  InsertTransactionTableSetByLockMode(txn, lock_mode, oid);
  // printf("task[%d] lock %d success!\n", txn->GetTransactionId(), oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // check whether there are still rows being locked
  if (!((txn->GetSharedRowLockSet()->empty() || (*txn->GetSharedRowLockSet())[oid].empty()) &&
        (txn->GetExclusiveRowLockSet()->empty() || (*txn->GetExclusiveRowLockSet())[oid].empty()))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> req_queue_lock(it->second->latch_);
  if (it->second->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  LockMode lock_mode = LockMode::SHARED;

  for (auto req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end();) {
    if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT((*req_it)->granted_, "Must have been granted in unlock function!");
      lock_mode = (*req_it)->lock_mode_;
      delete (*req_it);
      req_it = it->second->request_queue_.erase(req_it);
      break;
    }
    ++req_it;
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    default:
      break;
  }

  EraseTransactionTableSetByLockMode(txn, lock_mode, oid);
  it->second->cv_.notify_all();
  // printf("task[%d] unlock %d success!\n", txn->GetTransactionId(), oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /* validate legality of lock mode and transaction state */
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // S/IS/SIX locks are not required under READ_UNCOMMITTED
    if (lock_mode == LockMode::SHARED) {
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
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // check whether holding the table lock
  bool holding_table_lock = false;
  if (lock_mode == LockMode::SHARED) {
    if ((txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end()) ||
        (txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end()) ||
        (txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) ||
        (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) ||
        (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
         txn->GetSharedIntentionExclusiveTableLockSet()->end())) {
      holding_table_lock = true;
    }
  } else {
    if ((txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) ||
        (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) ||
        (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
         txn->GetSharedIntentionExclusiveTableLockSet()->end())) {
      holding_table_lock = true;
    }
  }

  if (!holding_table_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  row_lock_map_latch_.lock();
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
    it = row_lock_map_.find(rid);
  }
  row_lock_map_latch_.unlock();

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
          (prev_lock_mode == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // above has ensured that the valid table lock is held, so just do an upgrade is ok here
      it->second->upgrading_ = txn->GetTransactionId();
      delete (*req_it);
      it->second->request_queue_.erase(req_it);

      EraseTransactionRowSetByLockMode(txn, prev_lock_mode, oid, rid);

      is_upgraded = true;
      break;
    }
  }

  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             (!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                            [is_upgraded](LockRequest *req) {
                              return (is_upgraded && (!(req->granted_)));
                            }))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    case LockMode::SHARED:
      while ((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
             ((!(std::all_of(it->second->request_queue_.begin(), it->second->request_queue_.end(),
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) || req->lock_mode_ == LockMode::SHARED;
                             })))  // have another kind of lock
              && txn->GetState() != TransactionState::ABORTED)) {
        it->second->cv_.wait(req_queue_lock);
      }
      break;
    default:
      break;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    it->second->cv_.notify_all();
    return false;
  }

  auto new_req = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  new_req->granted_ = true;
  if (is_upgraded) {
    it->second->request_queue_.push_front(new_req);
    it->second->upgrading_ = INVALID_TXN_ID;
  } else {
    it->second->request_queue_.push_back(new_req);
  }

  InsertTransactionRowSetByLockMode(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> req_queue_lock(it->second->latch_);
  if (it->second->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  LockMode lock_mode = LockMode::SHARED;

  for (auto req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end();) {
    if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT((*req_it)->granted_, "Must have been granted in unlock function!");
      lock_mode = (*req_it)->lock_mode_;
      delete (*req_it);
      req_it = it->second->request_queue_.erase(req_it);
      break;
    }
    ++req_it;
  }

  // check whether holding the table lock
  bool holding_table_lock = false;
  if (lock_mode == LockMode::SHARED) {
    if ((txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end()) ||
        (txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end()) ||
        (txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) ||
        (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) ||
        (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
         txn->GetSharedIntentionExclusiveTableLockSet()->end())) {
      holding_table_lock = true;
    }
  } else {
    if ((txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) ||
        (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) ||
        (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
         txn->GetSharedIntentionExclusiveTableLockSet()->end())) {
      holding_table_lock = true;
    }
  }

  if (!holding_table_lock) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    default:
      break;
  }

  EraseTransactionRowSetByLockMode(txn, lock_mode, oid, rid);
  it->second->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  table_lock_map_latch_.lock();
  for (auto &tq : table_lock_map_) {
    for (auto it = tq.second->request_queue_.begin(); it != tq.second->request_queue_.end();) {
      delete (*it);
      it = tq.second->request_queue_.erase(it);
    }
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (auto &rq : row_lock_map_) {
    for (auto it = rq.second->request_queue_.begin(); it != rq.second->request_queue_.end();) {
      delete (*it);
      it = rq.second->request_queue_.erase(it);
    }
  }
  row_lock_map_latch_.unlock();
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
