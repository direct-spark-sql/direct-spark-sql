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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DateType}

case class DirectCurrentDate(timeZoneId: Option[String] = None)
    extends LeafExpression
    with TimeZoneAwareExpression {

  def this() = this(None)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis(), timeZone)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    ev.copy(code"int ${ev.value} = org.apache.spark.sql.execution.direct.DirectExecutionContext" +
      code".get().getCurrentDate(${tz});",
      isNull = FalseLiteral)
  }

  override def dataType: DataType = DateType

}
