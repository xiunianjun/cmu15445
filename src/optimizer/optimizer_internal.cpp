#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

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
  // the update case is too complex, so we simply choose not to perform optimization
  if (plan->GetType() == PlanType::Update || plan->GetType() == PlanType::Delete) {
    return plan;
  }

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
    // std::all_of(expr->GetChildren().begin(), expr->GetChildren().end(),
    //     [this, child_expressions, middle, left_size, right_size](const AbstractExpressionRef &child) {
    //       SplitExpression(child, child_expressions, middle, left_size, right_size);
    //       return true;
    //     });
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

}  // namespace bustub
