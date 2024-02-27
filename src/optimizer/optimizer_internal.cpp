#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/values_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

void OptimizerHelperFunction() {}

}  // namespace bustub

namespace bustub {

auto Optimizer::CheckIfColumnConstantConjunction(const AbstractExpressionRef &expr,
                                                 std::vector<uint32_t> *column_indexes,
                                                 std::vector<Value> *first_values, std::vector<Value> *last_values)
    -> bool {
  // if is "AND"
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    // check children
    return std::all_of(expr->GetChildren().begin(), expr->GetChildren().end(),
                       [this, column_indexes, first_values, last_values](const AbstractExpressionRef &child) {
                         return CheckIfColumnConstantConjunction(child, column_indexes, first_values, last_values);
                       });
  }

  // if is the comparison expr
  if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comparison_expr != nullptr) {
    // check that the children must be constant and column
    BUSTUB_ASSERT(comparison_expr->GetChildren().size() == 2, "Comparison expr must have exactly two children.");
    if (const auto *column_value_expr =
            dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildren()[0].get());
        column_value_expr != nullptr) {
      if (const auto *constant_value_expr =
              dynamic_cast<const ConstantValueExpression *>(comparison_expr->GetChildren()[1].get());
          constant_value_expr != nullptr) {
        bool exist = false;
        int idx = 0;
        for (auto ci : *column_indexes) {
          if (ci == column_value_expr->GetColIdx()) {
            exist = true;
            break;
          }
          idx++;
        }

        if (!exist) {
          // not exists
          idx = static_cast<int>(column_indexes->size());
          column_indexes->push_back(column_value_expr->GetColIdx());
          first_values->push_back(Value(TypeId::INTEGER, 0));
          last_values->push_back(Value(TypeId::INTEGER, 0));
        }

        std::vector<Column> columns;
        auto v = constant_value_expr->Evaluate(nullptr, Schema(columns));
        if (comparison_expr->comp_type_ == ComparisonType::GreaterThan ||
            comparison_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
          if ((*first_values)[idx].CompareLessThan(v) == CmpBool::CmpTrue) {
            (*first_values)[idx] = v;
          }
        }

        if (comparison_expr->comp_type_ == ComparisonType::LessThan ||
            comparison_expr->comp_type_ == ComparisonType::LessThanOrEqual) {
          if ((*last_values)[idx].CompareGreaterThan(v) == CmpBool::CmpTrue) {
            (*last_values)[idx] = v;
          }
        }

        if (comparison_expr->comp_type_ == ComparisonType::Equal) {
          (*first_values)[idx] = v;
          (*last_values)[idx] = v;
        }

        return true;
      }
    }

    return false;
  }

  return false;
}

auto Optimizer::OptimizeSeqscanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqscanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    // get column index, and value expression
    std::vector<uint32_t> column_indexes;
    std::vector<Value> first_values;
    std::vector<Value> last_values;
    if (seq_plan.filter_predicate_ != nullptr &&
        CheckIfColumnConstantConjunction(seq_plan.filter_predicate_, &column_indexes, &first_values, &last_values)) {
      // swicth to a index scan
      const auto *table_info = catalog_.GetTable(seq_plan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);

      for (const auto *index : indices) {
        const auto &columns = index->key_schema_.GetColumns();
        // check index key schema == order by columns
        bool valid = true;
        if (columns.size() == column_indexes.size()) {
          for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i].GetName() != table_info->schema_.GetColumn(column_indexes[i]).GetName()) {
              valid = false;
              break;
            }
          }
          if (valid) {
            if (std::all_of(first_values.begin(), first_values.end(), [](Value &n) {
                  return (n.CompareEquals(Value(TypeId::INTEGER, 0)) == CmpBool::CmpTrue);
                })) {
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index->index_oid_,
                                                         seq_plan.filter_predicate_);
            }
            Tuple first_t = Tuple(first_values, &(index->key_schema_));
            IntegerKeyType first_key;
            first_key.SetFromKey(first_t);

            if (std::all_of(last_values.begin(), last_values.end(), [](Value &n) {
                  return (n.CompareEquals(Value(TypeId::INTEGER, 0)) == CmpBool::CmpTrue);
                })) {
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index->index_oid_,
                                                         seq_plan.filter_predicate_);
            }
            Tuple last_t = Tuple(last_values, &(index->key_schema_));
            IntegerKeyType last_key;
            last_key.SetFromKey(last_t);
            return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index->index_oid_,
                                                       seq_plan.filter_predicate_, first_key, last_key);
          }
        }
      }
    }
  }
  return optimized_plan;
}

void Optimizer::SplitExpression(const AbstractExpressionRef &expr,
                                std::vector<AbstractExpressionRef> *child_expressions, AbstractExpressionRef *middle,
                                int left_size, int right_size) {
  // if is "AND"
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    // splite children
    for (auto &child : expr->GetChildren()) {
      SplitExpression(child, child_expressions, middle, left_size, right_size);
    }
    return;
  }

  // if is the comparison expr
  if (auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); comparison_expr != nullptr) {
    BUSTUB_ASSERT(comparison_expr->GetChildren().size() == 2, "Comparison expr must have exactly two children.");
    // for simple, assume that the left child must always be column value
    if (auto *left_column_value_expr =
            dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildren()[0].get());
        left_column_value_expr != nullptr) {
      auto left_tuple_idx = left_column_value_expr->GetTupleIdx();

      std::vector<AbstractExpressionRef> children;
      // if the right child is constant
      if (const auto *right_constant_value_expr =
              dynamic_cast<const ConstantValueExpression *>(comparison_expr->GetChildren()[1].get());
          right_constant_value_expr != nullptr) {
        if (!((*child_expressions)[left_tuple_idx])) {
          (*child_expressions)[left_tuple_idx] = std::make_shared<ComparisonExpression>(
              std::make_shared<ColumnValueExpression>(0, left_column_value_expr->GetColIdx(),
                                                      left_column_value_expr->GetReturnType()),
              comparison_expr->GetChildAt(1), comparison_expr->comp_type_);
        } else {
          (*child_expressions)[left_tuple_idx] = std::make_shared<LogicExpression>(
              (*child_expressions)[left_tuple_idx],
              std::make_shared<ComparisonExpression>(
                  std::make_shared<ColumnValueExpression>(0, left_column_value_expr->GetColIdx(),
                                                          left_column_value_expr->GetReturnType()),
                  comparison_expr->GetChildAt(1), comparison_expr->comp_type_),
              LogicType::And);
        }

        return;
      }

      // else, must be column value
      if (const auto *right_column_value_expr =
              dynamic_cast<const ColumnValueExpression *>(comparison_expr->GetChildren()[1].get());
          right_column_value_expr != nullptr) {
        auto right_tuple_idx = right_column_value_expr->GetTupleIdx();
        if (left_tuple_idx != right_tuple_idx) {
          if (!(*middle)) {
            *middle = std::make_shared<ComparisonExpression>(
                std::make_shared<ColumnValueExpression>(left_tuple_idx, left_column_value_expr->GetColIdx(),
                                                        left_column_value_expr->GetReturnType()),
                std::make_shared<ColumnValueExpression>(right_column_value_expr->GetTupleIdx(),
                                                        right_column_value_expr->GetColIdx(),
                                                        right_column_value_expr->GetReturnType()),
                comparison_expr->comp_type_);
          } else {
            *middle = std::make_shared<LogicExpression>(
                *middle,
                std::make_shared<ComparisonExpression>(
                    std::make_shared<ColumnValueExpression>(left_tuple_idx, left_column_value_expr->GetColIdx(),
                                                            left_column_value_expr->GetReturnType()),
                    std::make_shared<ColumnValueExpression>(right_column_value_expr->GetTupleIdx(),
                                                            right_column_value_expr->GetColIdx(),
                                                            right_column_value_expr->GetReturnType()),
                    comparison_expr->comp_type_),
                LogicType::And);
          }
        } else {
          if (!((*child_expressions)[left_tuple_idx])) {
            (*child_expressions)[left_tuple_idx] = std::make_shared<ComparisonExpression>(
                std::make_shared<ColumnValueExpression>(0, left_column_value_expr->GetColIdx(),
                                                        left_column_value_expr->GetReturnType()),
                std::make_shared<ColumnValueExpression>(0, right_column_value_expr->GetColIdx(),
                                                        right_column_value_expr->GetReturnType()),
                comparison_expr->comp_type_);
          } else {
            (*child_expressions)[left_tuple_idx] = std::make_shared<LogicExpression>(
                (*child_expressions)[left_tuple_idx],
                std::make_shared<ComparisonExpression>(
                    std::make_shared<ColumnValueExpression>(0, left_column_value_expr->GetColIdx(),
                                                            left_column_value_expr->GetReturnType()),
                    std::make_shared<ColumnValueExpression>(0, right_column_value_expr->GetColIdx(),
                                                            right_column_value_expr->GetReturnType()),
                    comparison_expr->comp_type_),
                LogicType::And);
          }
        }

        return;
      }
    }
  }

  if (!(*middle)) {
    *middle = expr;
  } else {
    *middle = std::make_shared<LogicExpression>(*middle, expr, LogicType::And);
  }
}

auto Optimizer::PredicatePushdown(const AbstractPlanNodeRef &plan, AbstractExpressionRef &expr) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    if (((!expr || expr->ToString() == "true") &&
         (!(nlj_plan.Predicate()) || nlj_plan.Predicate()->ToString() == "true"))) {
      return plan;
    }

    std::vector<AbstractExpressionRef> child_expressions(2, nullptr);
    AbstractExpressionRef middle = nullptr;

    auto left_size = nlj_plan.GetChildAt(0)->OutputSchema().GetColumns().size();
    auto right_size = nlj_plan.GetChildAt(1)->OutputSchema().GetColumns().size();

    if (nlj_plan.Predicate() != nullptr && nlj_plan.Predicate()->ToString() != "true") {
      SplitExpression(nlj_plan.Predicate(), &child_expressions, &middle, left_size, right_size);
    }

    if (expr != nullptr && expr->ToString() != "true") {
      expr = RewriteExpressionForJoin(expr, left_size, right_size);
      SplitExpression(expr, &child_expressions, &middle, left_size, right_size);
    }

    int i = -1;
    for (auto &child : nlj_plan.GetChildren()) {
      i++;
      if (child->GetType() != PlanType::MockScan) {
        children.emplace_back(PredicatePushdown(nlj_plan.GetChildAt(i), child_expressions[i]));
      } else {
        if (child_expressions[i] != nullptr) {
          children.emplace_back(std::make_shared<FilterPlanNode>(
              std::make_shared<const bustub::Schema>(child->OutputSchema()), child_expressions[i], child));
        } else {
          children.emplace_back(child);
        }
      }
    }

    auto return_plan = std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(nlj_plan.OutputSchema()),
                                                                nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), middle,
                                                                nlj_plan.GetJoinType());
    return return_plan->CloneWithChildren(std::move(children));
  }

  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredicatePushdown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (expr != nullptr) {
      return std::make_shared<FilterPlanNode>(
          std::make_shared<const bustub::Schema>(filter_plan.OutputSchema()),
          std::make_shared<LogicExpression>(filter_plan.GetPredicate(), expr, LogicType::And),
          filter_plan.GetChildAt(0));
    }

    return optimized_plan;
  }

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (expr != nullptr) {
      if (seq_plan.filter_predicate_ != nullptr) {
        return std::make_shared<SeqScanPlanNode>(
            std::make_shared<const bustub::Schema>(seq_plan.OutputSchema()), seq_plan.GetTableOid(),
            seq_plan.table_name_, std::make_shared<LogicExpression>(seq_plan.filter_predicate_, expr, LogicType::And));
      }
      return std::make_shared<SeqScanPlanNode>(std::make_shared<const bustub::Schema>(seq_plan.OutputSchema()),
                                               seq_plan.GetTableOid(), seq_plan.table_name_, expr);
    }

    return optimized_plan;
  }

  return optimized_plan;
}

auto Optimizer::OptimizePredicatePushdown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::shared_ptr<bustub::AbstractExpression> expr = nullptr;
  return PredicatePushdown(plan, expr);
}

auto Optimizer::CheckFilterPredicateStillFalse(const AbstractExpressionRef &expr) -> bool {
  // if is "AND"
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      // AND
      for (auto &child : expr->GetChildren()) {
        if (CheckFilterPredicateStillFalse(child)) {
          return true;
        }
      }
    } else {
      // OR
      return std::all_of(expr->GetChildren().begin(), expr->GetChildren().end(),
                         [this](const AbstractExpressionRef &child) { return CheckFilterPredicateStillFalse(child); });
    }
  }

  // if is the comparison expr
  if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comparison_expr != nullptr) {
    // check that the children must be constant and column
    BUSTUB_ASSERT(comparison_expr->GetChildren().size() == 2, "Comparison expr must have exactly two children.");
    if (const auto *left_constant_value_expr =
            dynamic_cast<const ConstantValueExpression *>(comparison_expr->GetChildren()[0].get());
        left_constant_value_expr != nullptr) {
      if (const auto *right_constant_value_expr =
              dynamic_cast<const ConstantValueExpression *>(comparison_expr->GetChildren()[1].get());
          right_constant_value_expr != nullptr) {
        std::vector<Column> tmp_col;
        Schema tmp_schema(tmp_col);
        if (!(comparison_expr->Evaluate(nullptr, tmp_schema).GetAs<bool>())) {
          return true;
        }
      }
    }

    return false;
  }

  return false;
}

auto Optimizer::OptimizeStillFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    BUSTUB_ASSERT(plan->children_.size() == 1, "must have exactly one children");

    if (CheckFilterPredicateStillFalse(filter_plan.GetPredicate())) {
      std::vector<AbstractPlanNodeRef> children;
      children.push_back(std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(filter_plan.OutputSchema()),
                                                          std::vector<std::vector<AbstractExpressionRef>>()));
      return plan->CloneWithChildren(children);
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeStillFalseFilter(child));
  }

  return plan->CloneWithChildren(std::move(children));
}

void Optimizer::GetAllColumnsFromExpr(const AbstractExpressionRef &expr, std::vector<uint32_t> *needed_column_idx) {
  // check child first
  for (const auto &child : expr->GetChildren()) {
    GetAllColumnsFromExpr(child, needed_column_idx);
  }

  // then check current node
  // if is the column expr
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    needed_column_idx->push_back(column_value_expr->GetColIdx());
  }
}

auto Optimizer::ChangeColumnsAccordingToPositionMap(const AbstractExpressionRef &expr, uint32_t *position_map,
                                                    uint32_t offset) -> AbstractExpressionRef {
  // check child first
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.push_back(ChangeColumnsAccordingToPositionMap(child, position_map, offset));
  }

  // then check current node
  // if is the column expr
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    return std::make_shared<ColumnValueExpression>(column_value_expr->GetTupleIdx(),
                                                   position_map[column_value_expr->GetColIdx()],
                                                   column_value_expr->GetReturnType());
  }

  return expr->CloneWithChildren(children);
}

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // from top to buttom
  // for simple, just check the projection-projection and projection-agg case...

  // the projection-projection case
  if (plan->GetType() == PlanType::Projection) {
    /* Eliminate useless aggragation columns and merge two projection, such as:
      select d1, d2 from
        select max(v1) as d1, max(v1) + max(v2) as d2, max(v3) as d3 from t1
    -> (eliminate calculation of d3)
      select max(v1), max(v1) + max(v2) from t1
    */
    auto &parent_projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
    BUSTUB_ENSURE(plan->children_.size() == 1, "Projection with multiple children?? That's weird!");
    if (parent_projection_plan.GetChildAt(0)->GetType() == PlanType::Projection) {
      auto &child_plan = dynamic_cast<const ProjectionPlanNode &>(*(parent_projection_plan.GetChildAt(0)));
      std::vector<AbstractExpressionRef> expressions;
      for (auto &expr : parent_projection_plan.GetExpressions()) {
        // for simple, just deal with the case of all column value
        if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
            column_value_expr != nullptr) {
          expressions.push_back(child_plan.GetExpressions()[column_value_expr->GetColIdx()]);
        } else {
          return plan;
        }
      }

      // deal with itself again
      return OptimizeColumnPruning(std::make_shared<ProjectionPlanNode>(
          std::make_shared<Schema>(parent_projection_plan.OutputSchema()), expressions, child_plan.GetChildAt(0)));
    }

    // the projection-agg case
    if (parent_projection_plan.GetChildAt(0)->GetType() == PlanType::Aggregation) {
      auto &child_plan = dynamic_cast<const AggregationPlanNode &>(*(parent_projection_plan.GetChildAt(0)));
      std::vector<AbstractExpressionRef> expressions;
      std::vector<AbstractExpressionRef> new_expressions;
      std::vector<AggregationType> agg_types;
      std::vector<AggregationType> new_agg_types;
      std::vector<Column> columns;
      std::vector<Column> new_columns;
      std::vector<uint32_t> needed_column_idx;

      /* First, eliminate useless aggragation columns, such as:
          select d1, d2 from
            select max(v1) as d1, max(v1) + max(v2) as d2, max(v3) as d3 from t1
        -> (eliminate calculation of d3)
          select d1, d2 from select max(v1) as d1, max(v1) + max(v2) as d2 from t1
      */
      // Ten days later I think this segment of code is not neccessary
      // because this case is done by the projection-projection case.
      // But it's troublesome to edit the code so it can just be understood
      // as a simple copy behaviour to the child_plan.aggrations and schema.
      for (auto &expr : parent_projection_plan.GetExpressions()) {
        GetAllColumnsFromExpr(expr, &needed_column_idx);
      }

      // only deal with these cases:
      // select max(v1), min(v2), count(*) ...;   only project aggragation columns
      // select v2, max(v1), ... order by v2;     project only order by and aggragation columns, with order-by columns
      // in front
      for (uint32_t i = 0; i < child_plan.GetGroupBys().size(); i++) {
        columns.push_back(child_plan.OutputSchema().GetColumn(i));
      }
      for (auto &idx : needed_column_idx) {
        if (idx < child_plan.GetGroupBys().size()) {
          continue;
        }
        uint32_t agg_idx = idx - child_plan.GetGroupBys().size();
        expressions.push_back(child_plan.GetAggregateAt(agg_idx));
        agg_types.push_back(child_plan.GetAggregateTypes()[agg_idx]);
        columns.push_back(child_plan.OutputSchema().GetColumn(idx));
      }

      /* Then, eliminate the inner duplicate of aggragation.
        After that, the sql will be:
          select d1, d1 + d2 from
            select max(v1) as d1, max(v2) as d2 from t1
      */
      uint32_t position_map[child_plan.GetGroupBys().size() + expressions.size()];
      for (uint32_t i = 0; i < child_plan.GetGroupBys().size(); i++) {
        position_map[i] = i;
      }

      for (uint32_t i = 0; i < expressions.size(); i++) {
        bool is_find = false;
        uint32_t j = 0;
        for (j = 0; j < new_expressions.size(); j++) {
          if (new_agg_types[j] == agg_types[i] && new_expressions[j]->ToString() == expressions[i]->ToString()) {
            is_find = true;
            break;
          }
        }

        if (!is_find) {
          new_expressions.push_back(expressions[i]);
          new_agg_types.push_back(agg_types[i]);
          position_map[i + child_plan.GetGroupBys().size()] =
              new_expressions.size() - 1 + child_plan.GetGroupBys().size();
        } else {
          auto it = columns.begin();
          for (uint32_t m = 0; m < i + child_plan.GetGroupBys().size(); m++) {
            it++;
          }
          columns.erase(it);
          position_map[i + child_plan.GetGroupBys().size()] = j + child_plan.GetGroupBys().size();
        }
      }

      std::vector<AbstractExpressionRef> parent_exprs;
      for (auto &expr : parent_projection_plan.GetExpressions()) {
        parent_exprs.push_back(
            ChangeColumnsAccordingToPositionMap(expr, position_map, child_plan.GetGroupBys().size()));
      }

      return std::make_shared<ProjectionPlanNode>(
          std::make_shared<Schema>(parent_projection_plan.OutputSchema()), parent_exprs,
          std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(columns), child_plan.GetChildAt(0),
                                                child_plan.GetGroupBys(), new_expressions, new_agg_types));
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  return plan->CloneWithChildren(children);
}

}  // namespace bustub
