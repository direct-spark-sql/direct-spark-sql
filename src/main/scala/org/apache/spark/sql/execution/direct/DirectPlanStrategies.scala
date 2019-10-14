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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{
  logical,
  ExistenceJoin,
  InnerLike,
  JoinType,
  LeftAnti,
  LeftOuter,
  LeftSemi,
  RightOuter
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.internal.SQLConf

object DirectPlanStrategies {

  def strategies: Seq[Strategy] = Seq(BasicOperators, JoinSelection)

  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case NamedLocalRelation(output, _, name) =>
        DynamicLocalTableScanExec(output, name) :: Nil
      case _ => Nil
    }
  }

  object JoinSelection extends Strategy with PredicateHelper {

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    private def getBuildSide(
        wantToBuildLeft: Boolean,
        wantToBuildRight: Boolean,
        left: LogicalPlan,
        right: LogicalPlan): Option[BuildSide] = {
      if (wantToBuildLeft && wantToBuildRight) {
        Some(BuildLeft) // default build the left
      } else if (wantToBuildLeft) {
        Some(BuildLeft)
      } else if (wantToBuildRight) {
        Some(BuildRight)
      } else {
        None
      }
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) =>
        // createShuffleHashJoinForDirectMode
        getBuildSide(canBuildLeft(joinType), canBuildRight(joinType), left, right).map {
          buildSide =>
            Seq(
              joins.ShuffledHashJoinExec(
                leftKeys,
                rightKeys,
                joinType,
                buildSide,
                condition,
                planLater(left),
                planLater(right)))
        }.get

      // Pick BroadcastNestedLoopJoin
      case j @ logical.Join(left, right, joinType, condition) =>
        val buildSide =
          getBuildSide(canBuildLeft(joinType), canBuildRight(joinType), left, right).get
        joins.BroadcastNestedLoopJoinExec(
          planLater(left),
          planLater(right),
          buildSide,
          joinType,
          condition) :: Nil

//      // Pick CartesianProduct for InnerJoin
//      case logical.Join(left, right, _: InnerLike, condition) =>
//        joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil

      // --- Cases where this strategy does not apply ---------------------------------------------

      case _ => Nil
    }
  }
}
