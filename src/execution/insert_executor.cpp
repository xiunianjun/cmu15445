//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  std::vector<Column> tmp_colum;
  tmp_colum.emplace_back("name", TypeId::INTEGER);
  one_value_schema_ = new Schema(tmp_colum);
}

InsertExecutor::~InsertExecutor() { delete one_value_schema_; }

void InsertExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  try {
    bool lock_success =
        exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!lock_success) {
      throw ExecutionException("fail to get table lock in the insert.");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("fail to get table lock in the insert.");
  }

  // initialize
  child_executor_->Init();
  insert_num_ = 0;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *param_tuple, RID *param_rid) -> bool {
  if (insert_num_ == -1) {
    return false;
  }

  // insert into table
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto table_heap = table_info->table_.get();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    try {
      rid = table_heap->InsertTuple(TupleMeta(), tuple, exec_ctx_->GetLockManager(), txn_, plan_->TableOid()).value();
    } catch (TransactionAbortException &e) {
      throw ExecutionException("fail to get row lock in the insert.");
    }

    insert_num_++;
    txn_->AppendTableWriteRecord({plan_->TableOid(), rid, table_heap});

    // update indexes
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto index_info : indexes) {
      index_info->index_->InsertEntry(
          tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), rid,
          exec_ctx_->GetTransaction());
      txn_->AppendIndexWriteRecord(
          {rid, plan_->TableOid(), WType::INSERT, tuple, index_info->index_oid_, exec_ctx_->GetCatalog()});
    }
  }

  std::vector<Value> tmp_val;
  tmp_val.emplace_back(TypeId::INTEGER, insert_num_);

  *param_tuple = Tuple(tmp_val, one_value_schema_);

  insert_num_ = -1;
  return true;
}

}  // namespace bustub
