//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  std::vector<Column> tmp_colum;
  tmp_colum.emplace_back("name", TypeId::INTEGER);
  one_value_schema_ = new Schema(tmp_colum);
}
DeleteExecutor::~DeleteExecutor() { delete one_value_schema_; }

void DeleteExecutor::Init() {
  // initialize
  child_executor_->Init();
  delete_num_ = 0;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *param_tuple, RID *param_rid) -> bool {
  if (delete_num_ == -1) {
    return false;
  }

  // insert into table
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto table_heap = table_info->table_.get();
  std::optional<Tuple> last_tuple = std::nullopt;
  RID last_rid;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (!(last_tuple.has_value())) {
      last_tuple = tuple;
      last_rid = rid;
      continue;
    }

    // delete
    auto meta = table_heap->GetTupleMeta(last_rid);
    meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(meta, last_rid);
    // update indexes
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto index_info : indexes) {
      index_info->index_->DeleteEntry(last_tuple.value().KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                                      index_info->index_->GetKeyAttrs()),
                                      last_rid, exec_ctx_->GetTransaction());
    }

    delete_num_++;
    last_tuple = tuple;
    last_rid = rid;
  }

  if (last_tuple.has_value()) {
    // delete
    auto meta = table_heap->GetTupleMeta(last_rid);
    meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(meta, last_rid);
    // update indexes
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto index_info : indexes) {
      index_info->index_->DeleteEntry(last_tuple.value().KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                                      index_info->index_->GetKeyAttrs()),
                                      last_rid, exec_ctx_->GetTransaction());
    }

    delete_num_++;
  }

  std::vector<Value> tmp_val;
  tmp_val.emplace_back(TypeId::INTEGER, delete_num_);

  *param_tuple = Tuple(tmp_val, one_value_schema_);

  delete_num_ = -1;
  return true;
}

}  // namespace bustub
