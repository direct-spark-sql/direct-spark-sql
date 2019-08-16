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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Stopwatch
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, BoundReference, Expression, InterpretedPredicate, MutableProjection, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.{Predicate => GenPredicate, _}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.direct.general.ExecSubqueryExpression
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType

abstract class DirectPlan extends QueryPlan[DirectPlan] with Logging {

  /**
   * A handle to the SQL Context that was used to create this plan. Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient final val sqlContext = DirectExecutionContext.get().activeSparkSession.sqlContext

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when SparkPlan nodes are created without the active sessions.
  val subexpressionEliminationEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.subexpressionEliminationEnabled
  } else {
    false
  }

  // whether we should fallback when hitting compilation errors caused by codegen
  private val codeGenFallBack = (sqlContext == null) || sqlContext.conf.codegenFallback

  /**
   * @return All metrics containing metrics of this DirectPlan,
   * which was stored in current ThreadContext
   */
  def metrics(): scala.collection.mutable.Map[String, SQLMetric] = {
    DirectExecutionContext
      .get()
      .planMetricsMap
      .getOrElseUpdate(this, scala.collection.mutable.Map[String, SQLMetric]())

  }

  /**
   * @return [[SQLMetric]] for the `name`.
   */
  def longMetric(name: String, metricValue: SQLMetric = null): SQLMetric = {
    val planMetrics = metrics()
    planMetrics.getOrElseUpdate(name, metricValue)
  }

  /**
   * List of (uncorrelated scalar subquery, future holding the subquery result) for this plan node.
   * This list is populated by [[prepareSubqueries]], which is called in [[prepare]].
   */
  private def runningSubqueries: ArrayBuffer[ExecSubqueryExpression] = {
    DirectExecutionContext
      .get()
      .runningSubQueriesMap
      .getOrElseUpdate(this, new ArrayBuffer[ExecSubqueryExpression])
  }

  /**
   * Finds scalar subquery expressions in this plan node and starts evaluating them.
   */
  protected def prepareSubqueries(): Unit = {
    expressions.foreach(m => {
      m.collect {
        case e: ExecSubqueryExpression =>
          e.plan.prepare()
          runningSubqueries += e
      }
    })
  }

  /**
   * Blocks the thread until all subqueries finish evaluation and update the results.
   */
  protected def waitForSubqueries(): Unit = synchronized {
    // fill in the result of subqueries
    runningSubqueries.foreach { sub =>
      sub.updateResult()
    }
    runningSubqueries.clear()
  }

  /**
   * Whether the "prepare" method is called.
   */
  private def prepared: Boolean = DirectExecutionContext.get().preparedMap.getOrElse(this, false)

  private def markPrepared(): Unit = {
    DirectExecutionContext.get().preparedMap.put(this, true)
  }

  protected def doPrepare(): Unit = {}

  /**
   * Prepares this DirectPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    // doPrepare() may depend on it's children, we should call prepare() on all the children first.
//    children.foreach(_.prepare())
//    synchronized {
//      if (!prepared) {
//        prepareSubqueries()
//        doPrepare()
//        markPrepared()
//      }
//    }
  }

  protected def doExecute(): Iterator[InternalRow]

  final def execute(): Iterator[InternalRow] = {
    prepare()
    waitForSubqueries()
    doExecute()
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean = false): MutableProjection = {
    log.debug(s"Creating MutableProj: $expressions, inputSchema: $inputSchema")
    MutableProjection.create(expressions, inputSchema)
  }

  private def genInterpretedPredicate(
      expression: Expression,
      inputSchema: Seq[Attribute]): InterpretedPredicate = {
    val str = expression.toString
    val logMessage = if (str.length > 256) {
      str.substring(0, 256 - 3) + "..."
    } else {
      str
    }
    logWarning(s"Codegen disabled for this expression:\n $logMessage")
    InterpretedPredicate.create(expression, inputSchema)
  }

  protected def newPredicate(
      expression: Expression,
      inputSchema: Seq[Attribute]): GenPredicate = {
    try {
      GeneratePredicate.generate(expression, inputSchema)
    } catch {
      case _ @(_: InternalCompilerException | _: CompileException) if codeGenFallBack =>
        genInterpretedPredicate(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder],
      inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    GenerateOrdering.generate(order, inputSchema)
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

trait LeafDirectExecNode extends DirectPlan {
  override final def children: Seq[DirectPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

object UnaryDirectExecNode {
  def unapply(a: Any): Option[(DirectPlan, DirectPlan)] = a match {
    case s: DirectPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}

trait UnaryDirectExecNode extends DirectPlan {
  def child: DirectPlan

  override final def children: Seq[DirectPlan] = child :: Nil
}

trait BinaryDirectExecNode extends DirectPlan {
  def left: DirectPlan
  def right: DirectPlan

  override final def children: Seq[DirectPlan] = Seq(left, right)
}

case class DirectPlanAdapter(sparkPlan: SparkPlan) extends DirectPlan {

  override def output: Seq[Attribute] = sparkPlan.output

  override def children: Seq[DirectPlan] = Nil

  override def doExecute(): Iterator[InternalRow] = {
    val s = new Stopwatch().start()
    val r = sparkPlan.executeCollect()
    s.stop()
    logWarning(
      "sparkPlan execute spend " + s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + ", " + sparkPlan)

    r.toIterator
  }

}
