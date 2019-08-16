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

import java.util.Properties
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, IsNotNull, NamedExpression, NullIntolerant, PredicateHelper, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.Predicate
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.direct.{DirectExecutionContext, DirectPlan, DirectSQLMetrics, UnaryDirectExecNode}
import org.apache.spark.util.ThreadUtils

case class ProjectDirectExec(projectList: Seq[NamedExpression], child: DirectPlan)
    extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def doExecute(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      val project: UnsafeProjection = {
        val project = UnsafeProjection.create(projectList, child.output)
        project.initialize(0)
        project
      }
      val childIter: Iterator[InternalRow] = child.execute()

      override def hasNext: Boolean = {
        childIter.hasNext
      }

      override def next: InternalRow = {
        val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
        numOutputRows += 1
        project(childIter.next())
      }

    }
  }

}

case class FilterDirectExec(condition: Expression, child: DirectPlan)
    extends UnaryDirectExecNode
    with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override def doExecute(): Iterator[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
    new Iterator[InternalRow] {

      val predicate: Predicate = {
        val predicate: Predicate = newPredicate(condition, child.output)
        predicate.initialize(0)
        predicate
      }
      val childIter: Iterator[InternalRow] = child.execute()
      val DUMMY_ROW = new GenericInternalRow(0)
      var nextRow: InternalRow = DUMMY_ROW

      override def hasNext: Boolean = {
        if (nextRow != DUMMY_ROW) {
          return true
        }
        while (childIter.hasNext) {
          nextRow = childIter.next()
          if (predicate.eval(nextRow)) {
            return true
          }
        }
        nextRow = DUMMY_ROW
        false
      }

      override def next: InternalRow = {
        if (nextRow != DUMMY_ROW || hasNext) {
          val res = nextRow
          numOutputRows += 1
          nextRow = DUMMY_ROW
          res
        } else {
          throw new NoSuchElementException
        }
      }
    }

  }

}

/**
 * Parent class for different types of subquery plans
 */
abstract class BaseSubqueryDirectExec extends DirectPlan {
  def name: String
  def child: DirectPlan

  override def output: Seq[Attribute] = child.output

}

/**
 * Physical plan for a subquery.
 */
case class SubqueryDirectExec(name: String, child: DirectPlan)
    extends BaseSubqueryDirectExec
    with UnaryDirectExecNode {

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        val beforeCollect = System.nanoTime()
        try {
          DirectExecutionContext.get()
          // Note that we use .executeCollect() because we don't want to convert data to Scala types
          val taskMemoryManager = new TaskMemoryManager(
            new UnifiedMemoryManager(
              new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
              Long.MaxValue,
              Long.MaxValue / 2,
              1),
            0)
          // prepare a TaskContext for execution
          TaskContext.setTaskContext(
            new TaskContextImpl(0, 0, 0, 0, 0, taskMemoryManager, new Properties, null))
          val rows: Array[InternalRow] = child.execute().toArray
          val beforeBuild = System.nanoTime()
          longMetric("collectTime", DirectSQLMetrics.createMetric()) += NANOSECONDS.toMillis(
            beforeBuild - beforeCollect)
          val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
          longMetric("dataSize", DirectSQLMetrics.createSizeMetric()) += dataSize
          rows
        } finally {
          DirectExecutionContext.get().markCompleted()
          TaskContext.unset()
          DirectExecutionContext.unset()
        }
      }
    }(SubqueryDirectExec.executionContext)
  }

  override def doPrepare(): Unit = {
    relationFuture
  }

  override def doExecute(): Iterator[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf).iterator
  }
}

case class LimitDirectExec(limit: Int, child: DirectPlan) extends UnaryDirectExecNode {
  override def doExecute(): Iterator[InternalRow] = {
    child.execute().take(limit)
  }
  override def output: Seq[Attribute] = child.output
}

object SubqueryDirectExec {
  private[execution] val executionContext =
    ExecutionContext.fromExecutorService(ThreadUtils.newDaemonFixedThreadPool(16, "subquery"))
}
