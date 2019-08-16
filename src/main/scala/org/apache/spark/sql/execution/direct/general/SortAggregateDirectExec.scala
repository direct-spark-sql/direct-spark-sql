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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.aggregate.SortBasedAggregationIterator
import org.apache.spark.sql.execution.direct.{DirectPlan, DirectSQLMetrics, UnaryDirectExecNode}

/**
 * Sort-based aggregate operator.
 */
case class SortAggregateDirectExec(
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

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def doExecute(): Iterator[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
    val iter = child.execute()
    // Because the constructor of an aggregation iterator will read at least the first row,
    // we need to get the value of iter.hasNext first.
    val hasInput = iter.hasNext
    val res = if (!hasInput && groupingExpressions.nonEmpty) {
      // This is a grouped aggregate and the input iterator is empty,
      // so return an empty iterator.
      Iterator.empty
    } else {
      val outputIter = new SortBasedAggregationIterator(
        0,
        groupingExpressions,
        child.output,
        iter,
        aggregateExpressions,
        aggregateAttributes,
        initialInputBufferOffset,
        resultExpressions,
        (expressions, inputSchema) =>
          newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
        numOutputRows)
      if (!hasInput && groupingExpressions.isEmpty) {
        // There is no input and there is no grouping expressions.
        // We need to output a single row as the output.
        numOutputRows += 1
        Iterator[UnsafeRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
      } else {
        outputIter
      }
    }
    res
  }

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"SortAggregate(key=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"SortAggregate(key=$keyString, functions=$functionString)"
    }
  }
}
