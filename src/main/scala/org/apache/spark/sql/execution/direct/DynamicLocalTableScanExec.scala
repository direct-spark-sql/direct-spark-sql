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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project, SubqueryAlias}
import org.apache.spark.sql.execution.{InputRDDCodegen, LeafExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics

case class DynamicLocalTableScanExec(output: Seq[Attribute], name: TableIdentifier)
    extends LeafExecNode {

  private lazy val rows: Seq[InternalRow] = {
    val foundRelation = SparkSession.active.sessionState.catalog.lookupRelation(name)
    foundRelation match {
      case SubqueryAlias(_, Project(_, LocalRelation(_, data, _))) =>
        data
      case SubqueryAlias(_, LocalRelation(_, data, _)) =>
        data
      case other => throw new RuntimeException("unexpected Relation[" + other + "]")
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val unsafeRows: Array[InternalRow] = {
    if (rows.isEmpty) {
      Array.empty
    } else {
      val proj = UnsafeProjection.create(output, output)
      rows.map(r => proj(r).copy()).toArray
    }
  }

  private lazy val numParallelism: Int =
    math.min(math.max(unsafeRows.length, 1), sqlContext.sparkContext.defaultParallelism)

  private val rdd = sqlContext.sparkContext.parallelize(unsafeRows, numParallelism)

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      numOutputRows += 1
      r
    }
  }

  override protected def stringArgs: Iterator[Any] = {
    if (rows.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    longMetric("numOutputRows").add(unsafeRows.size)
    unsafeRows
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    val taken = unsafeRows.take(limit)
    longMetric("numOutputRows").add(taken.size)
    taken
  }
  
}
