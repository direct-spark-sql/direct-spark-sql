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

import java.text.DateFormat
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


abstract class DirectUnixTime
  extends BinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: DateFormat =
    try {
      DateTimeUtils.newDateFormat(constFormat.toString, timeZone)
    } catch {
      case NonFatal(_) => null
    }

  override def eval(input: InternalRow): Any = {
    val t = left.eval(input)
    if (t == null) {
      null
    } else {
      left.dataType match {
        case DateType =>
          DateTimeUtils.daysToMillis(t.asInstanceOf[Int], timeZone) / 1000L
        case TimestampType =>
          t.asInstanceOf[Long] / 1000000L
        case StringType if right.foldable =>
          if (constFormat == null || formatter == null) {
            null
          } else {
            try {
              formatter.parse(
                t.asInstanceOf[UTF8String].toString).getTime / 1000L
            } catch {
              case NonFatal(_) => null
            }
          }
        case StringType =>
          val f = right.eval(input)
          if (f == null) {
            null
          } else {
            val formatString = f.asInstanceOf[UTF8String].toString
            try {
              DateTimeUtils.newDateFormat(formatString, timeZone).parse(
                t.asInstanceOf[UTF8String].toString).getTime / 1000L
            } catch {
              case NonFatal(_) => null
            }
          }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    left.dataType match {
      case StringType if right.foldable =>
        val df = classOf[FastDateFormat].getName
        if (formatter == null) {
          ExprCode.forNullValue(dataType)
        } else {
          val formatterName = ctx.addReferenceObj("formatter",
            FastDateFormat.getInstance(constFormat.toString, timeZone, Locale.US), df)
          val eval1 = left.genCode(ctx)
          ev.copy(code = code"""
            ${eval1.code}
            boolean ${ev.isNull} = ${eval1.isNull};
            $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            if (!${ev.isNull}) {
              try {
                ${ev.value} = $formatterName.parse(${eval1.value}.toString()).getTime() / 1000L;
              } catch (java.text.ParseException e) {
                ${ev.isNull} = true;
              }
            }""")
        }
      case StringType =>
        val tz = ctx.addReferenceObj("timeZone", timeZone)
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.value} = $dtu.newDateFormat($format.toString(), $tz)
                .parse($string.toString()).getTime() / 1000L;
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            } catch (java.text.ParseException e) {
              ${ev.isNull} = true;
            }
          """
        })
      case TimestampType =>
        val eval1 = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = ${eval1.value} / 1000000L;
          }""")
      case DateType =>
        val tz = ctx.addReferenceObj("timeZone", timeZone)
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val eval1 = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.daysToMillis(${eval1.value}, $tz) / 1000L;
          }""")
    }
  }
}

/**
 * Converts time string with given pattern.
 * Deterministic version of [[UnixTimestamp]], must have at least one parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr[, pattern]) - Returns the UNIX timestamp of the given time.",
  examples = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460041200
  """,
  since = "1.6.0")
case class DirectToUnixTimestamp(
                            timeExp: Expression,
                            format: Expression,
                            timeZoneId: Option[String] = None)
  extends DirectUnixTime {

  def this(timeExp: Expression, format: Expression) = this(timeExp, format, None)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def prettyName: String = "to_unix_timestamp"
}

/**
 * Converts time string with given pattern.
 * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
 * to Unix time stamp (in seconds), returns null if fail.
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 * If the second parameter is missing, use "yyyy-MM-dd HH:mm:ss".
 * If no parameters provided, the first parameter will be current_timestamp.
 * If the first parameter is a Date or Timestamp instead of String, we will ignore the
 * second parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_([expr[, pattern]]) - Returns the UNIX timestamp of current or specified time.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       1476884637
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460041200
  """,
  since = "1.5.0")
case class DirectUnixTimestamp(
                                timeExp: Expression,
                                format: Expression,
                                timeZoneId: Option[String] = None)
  extends DirectUnixTime {

  def this(timeExp: Expression, format: Expression) = this(timeExp, format, None)

  override def left: Expression = timeExp
  override def right: Expression = format

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def prettyName: String = "unix_timestamp"
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
@ExpressionDescription(
  usage = "_FUNC_(unix_time, format) - Returns `unix_time` in the specified `format`.",
  examples = """
    Examples:
      > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss');
       1970-01-01 00:00:00
  """,
  since = "1.5.0")
case class DirectFromUnixTime(
                               sec: Expression,
                               format: Expression,
                               timeZoneId: Option[String] = None)
  extends BinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

  def this(sec: Expression, format: Expression) = this(sec, format, None)

  override def left: Expression = sec
  override def right: Expression = format

  override def prettyName: String = "from_unixtime"

  def this(unix: Expression) = {
    this(unix, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: DateFormat =
    try {
      DateTimeUtils.newDateFormat(constFormat.toString, timeZone)
    } catch {
      case NonFatal(_) => null
    }

  override def eval(input: InternalRow): Any = {
    val time = left.eval(input)
    if (time == null) {
      null
    } else {
      if (format.foldable) {
        if (constFormat == null || formatter == null) {
          null
        } else {
          try {
            UTF8String.fromString(formatter.format(
              new java.util.Date(time.asInstanceOf[Long] * 1000L)))
          } catch {
            case NonFatal(_) => null
          }
        }
      } else {
        val f = format.eval(input)
        if (f == null) {
          null
        } else {
          try {
            UTF8String.fromString(DateTimeUtils.newDateFormat(f.toString, timeZone)
              .format(new java.util.Date(time.asInstanceOf[Long] * 1000L)))
          } catch {
            case NonFatal(_) => null
          }
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val df = classOf[FastDateFormat].getName
    if (format.foldable) {
      if (formatter == null) {
        ExprCode.forNullValue(StringType)
      } else {
        val formatterName = ctx.addReferenceObj("formatter",
          FastDateFormat.getInstance(constFormat.toString, timeZone, Locale.US), df)
        val t = left.genCode(ctx)
        ev.copy(code = code"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            try {
              ${ev.value} = UTF8String.fromString($formatterName.format(
                new java.util.Date(${t.value} * 1000L)));
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            }
          }""")
      }
    } else {
      val tz = ctx.addReferenceObj("timeZone", timeZone)
      val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
      nullSafeCodeGen(ctx, ev, (seconds, f) => {
        s"""
        try {
          ${ev.value} = UTF8String.fromString($dtu.newDateFormat($f.toString(), $tz).format(
            new java.util.Date($seconds * 1000L)));
        } catch (java.lang.IllegalArgumentException e) {
          ${ev.isNull} = true;
        }"""
      })
    }
  }
}
