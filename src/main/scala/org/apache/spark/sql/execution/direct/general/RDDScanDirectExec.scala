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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.direct.{DirectSQLMetrics, LeafDirectExecNode}
import org.apache.spark.util.Utils

/** Physical plan node for scanning data from an RDD of InternalRow. */
case class RDDScanDirectExec(output: Seq[Attribute], rdd: RDD[InternalRow], name: String)
    extends LeafDirectExecNode {

  private def rddName: String = Option(rdd.name).map(n => s" $n").getOrElse("")

  override val nodeName: String = s"Scan $name$rddName"

  override def doExecute(): Iterator[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createSizeMetric())
    val proj = UnsafeProjection.create(schema)
    proj.initialize(0)
    numOutputRows += 1
    Seq(proj(RDDScanDirectExec.oneRow)).iterator
  }

  override def simpleString: String = {
    s"$nodeName${Utils.truncatedString(output, "[", ",", "]")}"
  }
}

object RDDScanDirectExec {
  private val oneRow = InternalRow()
}
