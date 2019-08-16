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

package org.apache.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.direct.JavaTypeConverter
import org.apache.spark.sql.types.StructType

case class DirectDataTable(schema: StructType, data: Seq[Row]) {

  def toJavaMapList: java.util.List[java.util.Map[String, Any]] = {
    val columnsWithIdx = schema.map(_.name).zipWithIndex
    data
      .map(row => {
        val map: java.util.Map[String, Any] = new java.util.HashMap[String, Any]()
        columnsWithIdx.map {
          case (column, idx) => map.put(column, row.get(idx))
        }
        map
      })
      .asJava
  }

}

object DirectDataTable {

  def fromJavaMapList(
      schema: StructType,
      javaData: java.util.List[java.util.Map[String, Any]]): DirectDataTable = {
    val data = javaData.asScala.map(e =>
      Row.fromSeq(schema.map(f => JavaTypeConverter.convert(e.get(f.name), f.dataType))))
    DirectDataTable(schema, data)
  }
}
