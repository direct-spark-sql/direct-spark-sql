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

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{
  BaseSubqueryExec,
  SparkPlan,
  SQLExecution,
  SubqueryExec,
  UnaryExecNode
}
import org.apache.spark.util.ThreadUtils

case class SubqueryWithDirectChildExec(name: String, child: SparkPlan)
    extends BaseSubqueryExec
    with UnaryExecNode {

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        val directExecutePlan = DirectPlanConverter.convert(child)
        val rows: Array[InternalRow] = directExecutePlan.execute().toArray
        rows
      }
    }(SubqueryExec.executionContext)
  }

  protected override def doCanonicalize(): SparkPlan = {
    SubqueryExec("Subquery", child.canonicalized)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

}
