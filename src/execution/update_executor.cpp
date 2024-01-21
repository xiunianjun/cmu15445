//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  std::vector<Column> tmp_colum;
  tmp_colum.emplace_back("name", TypeId::INTEGER);
  one_value_schema_ = new Schema(tmp_colum);
}
UpdateExecutor::~UpdateExecutor() { delete one_value_schema_; }

void UpdateExecutor::Init() {
  // initialize
  child_executor_->Init();
  update_num_ = 0;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *param_tuple, RID *param_rid) -> bool {
  if (update_num_ == -1) {
    return false;
  }

  // insert into table
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto table_heap = table_info->table_.get();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    // delete first
    auto meta = table_heap->GetTupleMeta(rid);
    meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(meta, rid);
    // update indexes
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto index_info : indexes) {
      index_info->index_->DeleteEntry(
          tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), rid,
          exec_ctx_->GetTransaction());
    }

    // insert again
    std::vector<Value> insert_values;
    for (auto &exp : plan_->target_expressions_) {
      insert_values.push_back(exp->Evaluate(&tuple, table_info->schema_));
    }
    tuple = Tuple(insert_values, &(table_info->schema_));
    rid = table_heap->InsertTuple(TupleMeta(), tuple).value();

    // update indexes
    indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto index_info : indexes) {
      index_info->index_->InsertEntry(
          tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), rid,
          exec_ctx_->GetTransaction());
    }

    update_num_++;
  }

  std::vector<Value> tmp_val;
  tmp_val.push_back(Value(TypeId::INTEGER, update_num_));

  *param_tuple = Tuple(tmp_val, one_value_schema_);

  update_num_ = -1;
  return true;
}

}  // namespace bustub
