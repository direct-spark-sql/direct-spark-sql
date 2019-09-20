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

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project, SubqueryAlias}

case class LocalTableScanDirectExec(output: Seq[Attribute], name: TableIdentifier)
    extends LeafDirectExecNode {

  override def doExecute(): Iterator[InternalRow] = {
    val rows: Seq[InternalRow] = {
      val foundRelation =
        DirectExecutionContext.get().activeSparkSession.sessionState.catalog.lookupRelation(name)
      foundRelation match {
        case SubqueryAlias(_, Project(_, LocalRelation(_, data, _))) =>
          data
        case SubqueryAlias(_, LocalRelation(_, data, _)) =>
          data
        case other => throw new RuntimeException("unexpected Relation[" + other + "]")
      }
    }
//    val unsafeRows: Array[InternalRow] = {
//      if (rows.isEmpty) {
//        Array.empty
//      } else {
//        val proj = UnsafeProjection.create(output, output)
//        rows.map(r => proj(r).copy()).toArray
//      }
//    }
//    unsafeRows.toIterator
    rows.toIterator
  }
}
