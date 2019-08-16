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

import java.util.EventListener
import java.util.function.Supplier

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetric


object DirectExecutionContext {
  private[this] val executionContext: ThreadLocal[DirectExecutionContext] =
    ThreadLocal.withInitial[DirectExecutionContext]( new Supplier[DirectExecutionContext] {
      override def get(): DirectExecutionContext = {
        new DirectExecutionContext()
      }
    })

  def get(): DirectExecutionContext = executionContext.get

  def unset(): Unit = executionContext.remove()
}
class DirectExecutionContext {

  val planMetricsMap
    : scala.collection.mutable.Map[DirectPlan, scala.collection.mutable.Map[String, SQLMetric]] =
    scala.collection.mutable.Map[DirectPlan, scala.collection.mutable.Map[String, SQLMetric]]()

  val activeSparkSession: SparkSession = SparkSession.active

  private val onCompleteCallbacks = new ArrayBuffer[ExecutionCompletionListener]

  def addExecutionCompletionListener(listener: ExecutionCompletionListener): Unit = {
    onCompleteCallbacks += listener
  }

  def addExecutionCompletionListener(f: DirectExecutionContext => Unit): Unit = {
    addExecutionCompletionListener(new ExecutionCompletionListener {
      override def onExecutionCompletion(executionContext: DirectExecutionContext): Unit =
        f(executionContext)
    })
  }

  def markCompleted(): Unit = {
    onCompleteCallbacks.foreach(_.onExecutionCompletion(this))
  }

}

trait ExecutionCompletionListener extends EventListener {
  def onExecutionCompletion(executionContext: DirectExecutionContext): Unit
}
