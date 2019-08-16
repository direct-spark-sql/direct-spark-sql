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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, SortOrder, SortPrefix, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{SortPrefixUtils, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.direct.{DirectPlan, DirectSQLMetrics, UnaryDirectExecNode}

/**
 * Performs (external) sorting.
 *
 */
case class SortDirectExec(
    sortOrder: Seq[SortOrder],
    child: DirectPlan)
  extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = child.output

  def outputOrdering: Seq[SortOrder] = sortOrder

  private val enableRadixSort = sqlContext.conf.enableRadixSort


  def createSorter(): UnsafeExternalRowSorter = {
    val ordering = newOrdering(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
        SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix

      override def computePrefix(row: InternalRow):
      UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = UnsafeExternalRowSorter.create(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)
    sorter
  }

  override def doExecute(): Iterator[InternalRow] = {

    val peakMemory = longMetric("peakMemory", DirectSQLMetrics.createSizeMetric())
    val spillSize = longMetric("spillSize", DirectSQLMetrics.createSizeMetric())
    val sortTime = longMetric("sortTime", DirectSQLMetrics.createTimingMetric())

    val iter = child.execute()
    val sorter = createSorter()
    val metrics = TaskContext.get().taskMetrics()

    // Remember spill data size of this task before execute this operator so that we can
    // figure out how many bytes we spilled for this operator.
    val spillSizeBefore = metrics.memoryBytesSpilled
    val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
    sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
    peakMemory += sorter.getPeakMemoryUsage
    spillSize += metrics.memoryBytesSpilled - spillSizeBefore
    metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

    sortedIterator

  }

}
