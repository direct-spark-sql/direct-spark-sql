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

package org.apache.spark.sql.execution.direct.general

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, Expression, ExprId, Literal, PlanExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[BaseSubqueryDirectExec] {

  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit

  /** Updates the expression with a new plan. */
  override def withNewPlan(plan: BaseSubqueryDirectExec): ExecSubqueryExpression

  override def canonicalize(attrs: AttributeSeq): ExecSubqueryExpression = {
    withNewPlan(plan.canonicalized.asInstanceOf[BaseSubqueryDirectExec])
      .asInstanceOf[ExecSubqueryExpression]
  }
}

object ExecSubqueryExpression {

  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.find {
      case _: ExecSubqueryExpression => true
      case _ => false
    }.isDefined
  }
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarDirectSubquery(plan: BaseSubqueryDirectExec, exprId: ExprId)
    extends ExecSubqueryExpression {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryDirectExec): ScalarDirectSubquery = copy(plan = query)

  override def semanticEquals(other: Expression): Boolean = other match {
    case s: ScalarDirectSubquery => plan.sameResult(s.plan)
    case _ => false
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.execute().toArray
    if (rows.length > 1) {
      sys.error(s"more than one row returned by a subquery used as an expression:\n$plan")
    }
    if (rows.length == 1) {
      assert(
        rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      result = rows(0).get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}
