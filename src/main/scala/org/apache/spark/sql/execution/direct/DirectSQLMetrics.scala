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

import org.apache.spark.sql.execution.metric.SQLMetric

object DirectSQLMetrics {
  private val SUM_METRIC = "sum"
  private val SIZE_METRIC = "size"
  private val TIMING_METRIC = "timing"
  private val NS_TIMING_METRIC = "nsTiming"
  private val AVERAGE_METRIC = "average"

  def createMetric(): SQLMetric = {
    val acc = new SQLMetric(SUM_METRIC)
    acc
  }

  /**
   * Create a metric to report the size information (including total, min, med, max) like data size,
   * spill size, etc.
   */
  def createSizeMetric(): SQLMetric = {
    // The final result of this metric in physical operator UI may look like:
    // data size total (min, med, max):
    // 100GB (100MB, 1GB, 10GB)
    val acc = new SQLMetric(SIZE_METRIC, -1)
    acc
  }

  def createTimingMetric(): SQLMetric = {
    // The final result of this metric in physical operator UI may looks like:
    // duration(min, med, max):
    // 5s (800ms, 1s, 2s)
    val acc = new SQLMetric(TIMING_METRIC, -1)
    acc
  }

  def createNanoTimingMetric(): SQLMetric = {
    // Same with createTimingMetric, just normalize the unit of time to millisecond.
    val acc = new SQLMetric(NS_TIMING_METRIC, -1)
    acc
  }

  /**
   * Create a metric to report the average information (including min, med, max) like
   * avg hash probe. As average metrics are double values, this kind of metrics should be
   * only set with `SQLMetric.set` method instead of other methods like `SQLMetric.add`.
   * The initial values (zeros) of this metrics will be excluded after.
   */
  def createAverageMetric(): SQLMetric = {
    // The final result of this metric in physical operator UI may looks like:
    // probe avg (min, med, max):
    // (1.2, 2.2, 6.3)
    val acc = new SQLMetric(AVERAGE_METRIC)
    acc
  }
}
