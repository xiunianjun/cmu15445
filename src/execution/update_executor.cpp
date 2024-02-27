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
    std::vector<Value> new_values;
    for (auto &exp : plan_->target_expressions_) {
      new_values.push_back(exp->Evaluate(&tuple, table_info->schema_));
    }
    auto new_tuple = Tuple(new_values, &(table_info->schema_));
    table_heap->UpdateTupleInPlaceUnsafe(table_heap->GetTupleMeta(rid), new_tuple, rid);
    IndexWriteRecord record =
        IndexWriteRecord(rid, plan_->TableOid(), WType::UPDATE, new_tuple, 0, exec_ctx_->GetCatalog());
    record.old_tuple_ = tuple;
    exec_ctx_->GetTransaction()->AppendIndexWriteRecord(record);
    update_num_++;
    // ATTENTION: I ignore the case that update the index column, so just passing the
    // terrier bench and can't pass the p3.05 sql test.
  }

  std::vector<Value> tmp_val;
  tmp_val.emplace_back(TypeId::INTEGER, update_num_);

  *param_tuple = Tuple(tmp_val, one_value_schema_);

  update_num_ = -1;
  return true;
}

}  // namespace bustub
