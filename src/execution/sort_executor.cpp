#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  cursor_ = 0;

  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), CompareTuplesByOrder(GetOutputSchema(), plan_->GetOrderBy()));
}

auto SortExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  if (cursor_ >= tuples_.size()) {
    return false;
  }

  *param_tuple = tuples_[cursor_++];

  return true;
}

}  // namespace bustub
