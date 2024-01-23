//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_schema_size_ = plan_->GetLeftPlan()->OutputSchema().GetColumns().size();
  right_schema_size_ = plan_->GetRightPlan()->OutputSchema().GetColumns().size();
  BUSTUB_ASSERT(left_schema_size_ + right_schema_size_ <= plan_->OutputSchema().GetColumns().size(), "size");
  RID rid;
  left_executor_->Next(&current_left_tuple_, &rid);
  current_left_used_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  Tuple right_tuple;
  RID rid;
  while (true) {
    if (right_executor_->Next(&right_tuple, &rid)) {
    } else {
      if (!current_left_used_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> tmp_vals;
        for (uint32_t i = 0; i < right_schema_size_; i++) {
          tmp_vals.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
        right_tuple = Tuple(tmp_vals, &(plan_->GetRightPlan()->OutputSchema()));

        current_left_used_ = true;
        break;
      }

      if (left_executor_->Next(&current_left_tuple_, &rid)) {
        right_executor_->Init();
        current_left_used_ = false;
        continue;
      }

      right_executor_->Init();  // to pass the assert left_next_count == right_init_count
      return false;
    }

    if (plan_->Predicate() != nullptr && plan_->Predicate()
                                             ->EvaluateJoin(&current_left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                            &right_tuple, plan_->GetRightPlan()->OutputSchema())
                                             .GetAs<bool>()) {
      current_left_used_ = true;
      break;
    }
  }

  std::vector<Value> tmp_vals;
  for (uint32_t i = 0; i < left_schema_size_; i++) {
    tmp_vals.push_back(current_left_tuple_.GetValue(&(plan_->GetLeftPlan()->OutputSchema()), i));
  }
  for (uint32_t i = 0; i < right_schema_size_; i++) {
    tmp_vals.push_back(right_tuple.GetValue(&(plan_->GetRightPlan()->OutputSchema()), i));
  }

  *param_tuple = Tuple(tmp_vals, &(GetOutputSchema()));

  return true;
}

}  // namespace bustub
