#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // do for its child first, from buttom to up
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    // Has exactly one children
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 1, "Limit should have only 1 child.");
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);

    if (limit_plan.GetChildPlan()->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*(limit_plan.GetChildPlan()));
      return std::make_shared<TopNPlanNode>(std::make_shared<Schema>(sort_plan.OutputSchema()),
                                            sort_plan.GetChildPlan(), sort_plan.GetOrderBy(), limit_plan.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
