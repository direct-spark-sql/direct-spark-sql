/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.direct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.direct.general._
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, CartesianProductExec, HashJoin}
import org.apache.spark.sql.execution.window.{WindowDirectExec, WindowExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Plans scalar subqueries from that are present in the given [[SparkPlan]].
 */
case class PlanSubqueriesWithDirectChild(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
//
//        val sparkPlan = new QueryExecution(sparkSession, subquery.plan).sparkPlan
//        ScalarSubquery(
//          SubqueryWithDirectChildExec(s"scalar-subquery#${subquery.exprId.id}", sparkPlan),
//          subquery.exprId)
        throw new UnsupportedOperationException("ScalarSubquery is not supported now" + subquery)

    }
  }
}

/**
 *
 * ensure the requiredOrdering of SparkPlan
 */
case class EnsureOrdering(conf: SQLConf) extends Rule[SparkPlan] {
  def apply(operator: SparkPlan): SparkPlan = {
    operator.transformUp {
      case operator: SparkPlan =>
        var children: Seq[SparkPlan] = operator.children
        val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
        children = children.zip(requiredChildOrderings).map {
          case (child, requiredOrdering) =>
            if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
              child
            } else {
              SortExec(requiredOrdering, global = false, child = child)
            }
        }
        operator.withNewChildren(children)
    }
  }
}

object DirectPlanConverter {

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] =
    Seq(
      PlanSubqueriesWithDirectChild(DirectExecutionContext.get().activeSparkSession),
      EnsureOrdering(DirectExecutionContext.get().activeSparkSession.sessionState.conf),
      CollapseCodegenStages(DirectExecutionContext.get().activeSparkSession.sessionState.conf))

  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  def convert(plan: SparkPlan): DirectPlan = {
    val preparedSparkPlan = prepareForExecution(plan)
    val res = convertToDirectPlan(preparedSparkPlan)
    res

  }

  def convertToDirectPlan(plan: SparkPlan): DirectPlan = {
    val wholeStageEnabled =
      DirectExecutionContext.get().activeSparkSession.sqlContext.conf.wholeStageEnabled
    val codegenFallback =
      DirectExecutionContext.get().activeSparkSession.sqlContext.conf.codegenFallback

    plan match {

      case codegenExec: WholeStageCodegenExec if wholeStageEnabled =>
        DirectWholeStageCodegenExec(codegenExec)

      case inputAdapter: InputAdapter if wholeStageEnabled =>
        DirectInputAdapter(convertToDirectPlan(inputAdapter.child))

      case DynamicLocalTableScanExec(output, name) =>
        LocalTableScanDirectExec(output, name)

      // window
      case windowExec: WindowExec =>
        WindowDirectExec(
          windowExec.windowExpression,
          windowExec.partitionSpec,
          windowExec.orderSpec,
          convertToDirectPlan(windowExec.child))

      // sort
      case sortExec: SortExec =>
        SortDirectExec(sortExec.sortOrder, convertToDirectPlan(sortExec.child))

      // limit
      case localLimitExec: LocalLimitExec =>
        LimitDirectExec(localLimitExec.limit, convertToDirectPlan(localLimitExec.child))

      // join
      case hashJoin: HashJoin =>
        HashJoinDirectExec(
          hashJoin.leftKeys,
          hashJoin.rightKeys,
          hashJoin.joinType,
          hashJoin.condition,
          convertToDirectPlan(hashJoin.left),
          convertToDirectPlan(hashJoin.right))

      // aggregate
      case objectHashAggregateExec: ObjectHashAggregateExec =>
        ObjectHashAggregateDirectExec(
          objectHashAggregateExec.groupingExpressions,
          objectHashAggregateExec.aggregateExpressions,
          objectHashAggregateExec.aggregateAttributes,
          objectHashAggregateExec.initialInputBufferOffset,
          objectHashAggregateExec.resultExpressions,
          convertToDirectPlan(objectHashAggregateExec.child))

      case sortAggregateExec: SortAggregateExec =>
        SortAggregateDirectExec(
          sortAggregateExec.groupingExpressions,
          sortAggregateExec.aggregateExpressions,
          sortAggregateExec.aggregateAttributes,
          sortAggregateExec.initialInputBufferOffset,
          sortAggregateExec.resultExpressions,
          convertToDirectPlan(sortAggregateExec.child))

      case generateExec: GenerateExec =>
        GenerateDirectExec(
          generateExec.generator,
          generateExec.requiredChildOutput,
          generateExec.outer,
          generateExec.generatorOutput,
          convertToDirectPlan(generateExec.child))

      case unionExec: UnionExec =>
        UnionDirectExec(unionExec.children.map(convertToDirectPlan))

      case rddScanExec: RDDScanExec if "OneRowRelation".equals(rddScanExec.name) =>
        RDDScanDirectExec(rddScanExec.output, rddScanExec.rdd, rddScanExec.name)

      case hashAggregateExec: HashAggregateExec =>
        HashAggregateDirectExec(
          hashAggregateExec.groupingExpressions,
          hashAggregateExec.aggregateExpressions,
          hashAggregateExec.aggregateAttributes,
          hashAggregateExec.initialInputBufferOffset,
          hashAggregateExec.resultExpressions,
          convertToDirectPlan(hashAggregateExec.child))

      case ProjectExec(projectList, child) =>
        ProjectDirectExec(projectList, convertToDirectPlan(child))
      case FilterExec(condition, child) =>
        FilterDirectExec(condition, convertToDirectPlan(child))

      case expandExec: ExpandExec =>
        ExpandDirectExec(
          expandExec.projections,
          expandExec.output,
          convertToDirectPlan(expandExec.child))

      case broadcastNestedLoopJoinExec: BroadcastNestedLoopJoinExec =>
        DirectPlanAdapter(broadcastNestedLoopJoinExec)
      case cartesianProductExec: CartesianProductExec =>
        DirectPlanAdapter(cartesianProductExec)

      case other =>
        // DirectPlanAdapter(other)
        throw new UnsupportedOperationException("can't convert this SparkPlan " + other)
    }
  }

}
