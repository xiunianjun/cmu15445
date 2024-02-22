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
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
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

  if (!is_upgraded) {
    it->second->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
  }
  auto req_it = std::prev(it->second->request_queue_.end());
  if (is_upgraded) {
    req_it = it->second->request_queue_.end();
  }

  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_)));
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    case LockMode::SHARED:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) || req->lock_mode_ == LockMode::SHARED ||
                                      req->lock_mode_ == LockMode::INTENTION_SHARED;
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) ||
                                      req->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                                      req->lock_mode_ == LockMode::INTENTION_SHARED;
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    case LockMode::INTENTION_SHARED:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) || req->lock_mode_ != LockMode::EXCLUSIVE;
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) ||
                                      req->lock_mode_ == LockMode::INTENTION_SHARED;
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    default:
      break;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    if (is_upgraded) {
      it->second->upgrading_ = INVALID_TXN_ID;
    } else {
      delete (*req_it);
      it->second->request_queue_.erase(req_it);
    }
    it->second->cv_.notify_all();
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
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::SHRINKING);
      }
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

  if (!is_upgraded) {
    it->second->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
  }
  auto req_it = std::prev(it->second->request_queue_.end());
  if (is_upgraded) {
    req_it = it->second->request_queue_.end();
  }

  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_)));
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    case LockMode::SHARED:
      while (((it->second->upgrading_ != INVALID_TXN_ID && !is_upgraded) ||
              (!(std::all_of(it->second->request_queue_.begin(), req_it,
                             [is_upgraded](LockRequest *req) {
                               return (is_upgraded && (!(req->granted_))) || req->lock_mode_ == LockMode::SHARED;
                             }))))  // have another kind of lock
             && txn->GetState() != TransactionState::ABORTED) {
        it->second->cv_.wait(req_queue_lock);
        if (is_upgraded) {
          req_it = it->second->request_queue_.end();
        } else {
          for (req_it = it->second->request_queue_.begin(); req_it != it->second->request_queue_.end(); ++req_it) {
            if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
              break;
            }
          }
        }
      }
      break;
    default:
      break;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    if (is_upgraded) {
      it->second->upgrading_ = INVALID_TXN_ID;
    } else {
      delete (*req_it);
      it->second->request_queue_.erase(req_it);
    }
    it->second->cv_.notify_all();
    return false;
  }

  if (is_upgraded) {
    auto upgraded_req = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
    upgraded_req->granted_ = true;
    it->second->request_queue_.push_front(upgraded_req);
    it->second->upgrading_ = INVALID_TXN_ID;
  } else {
    (*req_it)->granted_ = true;
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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_[t1].find(t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &visited)
    -> bool {
  if (visited.find(source_txn) != visited.end()) {  // has cycle
    // delete no-circle prefix
    for (auto it = path.begin(); it != path.end();) {
      if (*it == source_txn) {
        break;
      }

      it = path.erase(it);
    }
    return true;
  }

  visited.insert(source_txn);
  path.push_back(source_txn);

  for (auto &edge : waits_for_[source_txn]) {
    if (FindCycle(edge, path, visited)) {
      return true;
    }
  }

  visited.erase(visited.find(source_txn));
  path.pop_back();
  return false;
}

void LockManager::PrintGraph() {
  printf("--------------------------\n");
  for (auto &pair : GetEdgeList()) {
    printf("(%d, %d)\n", pair.first, pair.second);
  }
  printf("--------------------------\n");
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  if (waits_for_.size() < 2) {
    return false;
  }

  std::vector<txn_id_t> path;
  std::unordered_set<txn_id_t> visited;

  for (auto &wait_pair : waits_for_) {
    path.clear();
    visited.clear();
    // PrintGraph();
    if (FindCycle(wait_pair.first, path, visited)) {
      goto HAS_CYCLE;
    }
  }

  return false;

HAS_CYCLE:
  *txn_id = *(path.begin());
  for (auto &id : path) {
    if (*txn_id < id) {
      *txn_id = id;
    }
  }

  return true;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &pair : waits_for_) {
    for (auto &edge : pair.second) {
      edges.emplace_back(pair.first, edge);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // TODO(students): detect deadlock
      // build wait-for graph
      std::unordered_map<txn_id_t, std::vector<std::shared_ptr<LockRequestQueue>>> sleep_for;

      table_lock_map_latch_.lock();
      for (auto &table_lock_pair : table_lock_map_) {
        for (auto &req_i : table_lock_pair.second->request_queue_) {
          if (req_i->granted_) {
            continue;
          }

          for (auto &req_j : table_lock_pair.second->request_queue_) {
            if (req_i->txn_id_ == req_j->txn_id_) {
              break;
            }

            if (req_j->granted_) {
              if ((req_i->lock_mode_ == LockMode::SHARED && req_j->lock_mode_ != LockMode::INTENTION_SHARED &&
                   req_j->lock_mode_ != LockMode::SHARED) ||
                  (req_i->lock_mode_ == LockMode::EXCLUSIVE) || (req_j->lock_mode_ == LockMode::EXCLUSIVE) ||
                  (req_i->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
                   req_j->lock_mode_ != LockMode::INTENTION_EXCLUSIVE &&
                   req_j->lock_mode_ != LockMode::INTENTION_SHARED) ||
                  (req_i->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE &&
                   req_j->lock_mode_ != LockMode::INTENTION_SHARED)) {
                AddEdge(req_i->txn_id_, req_j->txn_id_);
                sleep_for[req_i->txn_id_].push_back(table_lock_pair.second);
              }
            }
          }
        }
      }
      table_lock_map_latch_.unlock();

      row_lock_map_latch_.lock();
      for (auto &row_lock_pair : row_lock_map_) {
        for (auto &req_i : row_lock_pair.second->request_queue_) {
          if (req_i->granted_) {
            continue;
          }

          for (auto &req_j : row_lock_pair.second->request_queue_) {
            if (req_i->txn_id_ == req_j->txn_id_) {
              break;
            }

            if (req_j->granted_) {
              if ((req_i->lock_mode_ == LockMode::SHARED && req_j->lock_mode_ != LockMode::SHARED) ||
                  (req_i->lock_mode_ == LockMode::EXCLUSIVE) || (req_j->lock_mode_ == LockMode::EXCLUSIVE)) {
                AddEdge(req_i->txn_id_, req_j->txn_id_);
                sleep_for[req_i->txn_id_].push_back(row_lock_pair.second);
              }
            }
          }
        }
      }
      row_lock_map_latch_.unlock();

      // detect cycle and abort txn
      txn_id_t abort_txn;
      while (HasCycle(&abort_txn)) {
        // do an abort
        auto txn = txn_manager_->GetTransaction(abort_txn);
        txn->SetState(TransactionState::ABORTED);

        // notify related txns
        for (auto &lock_req : sleep_for[abort_txn]) {
          lock_req->cv_.notify_all();
        }
        sleep_for.erase(sleep_for.find(abort_txn));

        // delete all related edges
        for (auto it = waits_for_.begin(); it != waits_for_.end();) {
          if (it->first == abort_txn) {
            it = waits_for_.erase(it);
            continue;
          }

          RemoveEdge(it->first, abort_txn);

          ++it;
        }
      }

      // destory waot-for graph
      waits_for_.clear();
    }
  }
}
}  // namespace bustub
