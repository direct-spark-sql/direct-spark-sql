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

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, AttributeSet, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, TungstenAggregationIterator}
import org.apache.spark.sql.execution.direct.{DirectPlan, DirectSQLMetrics, UnaryDirectExecNode}

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class HashAggregateDirectExec(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: DirectPlan)
    extends UnaryDirectExecNode {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(HashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  // This is for testing. We force TungstenAggregationIterator to fall back to the unsafe row hash
  // map and/or the sort-based aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[(Int, Int)] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  override def doExecute(): Iterator[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
    val peakMemory = longMetric("peakMemory", DirectSQLMetrics.createSizeMetric())
    val spillSize = longMetric("spillSize", DirectSQLMetrics.createSizeMetric())
    val avgHashProbe = longMetric("avgHashProbe", DirectSQLMetrics.createAverageMetric())
    val aggTime = longMetric("aggTime", DirectSQLMetrics.createTimingMetric())

    val iter = child.execute()
    val beforeAgg = System.nanoTime()
    val hasInput = iter.hasNext
    val res = if (!hasInput && groupingExpressions.nonEmpty) {
      // This is a grouped aggregate and the input iterator is empty,
      // so return an empty iterator.
      Iterator.empty
    } else {
      val aggregationIterator =
        new TungstenAggregationIterator(
          0,
          groupingExpressions,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
          child.output,
          iter,
          testFallbackStartsAt,
          numOutputRows,
          peakMemory,
          spillSize,
          avgHashProbe)
      if (!hasInput && groupingExpressions.isEmpty) {
        numOutputRows += 1
        Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
      } else {
        aggregationIterator
      }
    }
    aggTime += NANOSECONDS.toMillis(System.nanoTime() - beforeAgg)
    res
  }

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
        val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
        val outputString = truncatedString(output, "[", ", ", "]", maxFields)
        if (verbose) {
          s"HashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
        } else {
          s"HashAggregate(keys=$keyString, functions=$functionString)"
        }
      case Some(fallbackStartsAt) =>
        s"HashAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }

}
