#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  CompareTuplesByOrder comparison_fn = CompareTuplesByOrder(GetOutputSchema(), plan_->GetOrderBy());
  heap_ = std::make_unique<std::priority_queue<Tuple, std::vector<Tuple>, CompareTuplesByOrder>>(comparison_fn);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    heap_->push(tuple);
    while (heap_->size() > plan_->GetN()) {
      heap_->pop();
    }
  }

  while (!(heap_->empty())) {
    tuples_.push_back(heap_->top());
    heap_->pop();
  }
}

auto TopNExecutor::Next(Tuple *param_tuple, RID *param_rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }

  *param_tuple = tuples_.back();
  tuples_.pop_back();

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
