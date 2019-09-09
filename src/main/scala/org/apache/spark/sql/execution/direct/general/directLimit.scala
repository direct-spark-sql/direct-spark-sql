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
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.execution.direct.{DirectPlan, UnaryDirectExecNode}
import org.apache.spark.util.Utils

case class LimitDirectExec(limit: Int, child: DirectPlan) extends UnaryDirectExecNode {
  override def doExecute(): Iterator[InternalRow] = {
    child.execute().take(limit)
  }
  override def output: Seq[Attribute] = child.output
}

case class TakeOrderedAndProjectDirectExec(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: DirectPlan)
    extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def doExecute(): Iterator[InternalRow] = {
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val topK: Iterator[InternalRow] = {
      val iter = child.execute().map(_.copy())
      org.apache.spark.util.collection.Utils.takeOrdered(iter, limit)(ord)
    }
    if (projectList != child.output) {
      val proj = UnsafeProjection.create(projectList, child.output)
      topK.map(r => proj(r))
    } else {
      topK
    }
  }

  override def simpleString: String = {
    val orderByString = Utils.truncatedString(sortOrder, "[", ",", "]")
    val outputString = Utils.truncatedString(output, "[", ",", "]")

    s"TakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}
