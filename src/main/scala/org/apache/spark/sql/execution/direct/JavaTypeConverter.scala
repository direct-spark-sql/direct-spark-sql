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

import java.math.{BigDecimal, BigInteger}
import java.sql.{Date, Timestamp}
import java.util.TimeZone

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.types._

object JavaTypeConverter {

  def toString(value: Any): String = value.toString

  def toBoolean(o: Any): Boolean =
    o match {
      case b: Boolean => b
      case n: Number => toBoolean(n)
      case s: String => toBoolean(s)
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to boolean")
    }

  def toBoolean(number: Number): Boolean = !(number == 0)

  def toBoolean(str: String): Boolean = str.trim.toBoolean

  def toByte(o: Any): Byte = {
    o match {
      case b: Byte => b
      case n: Number => toByte(n)
      case other => other.toString.toByte
    }
  }

  def toByte(number: Number): Byte = number.byteValue

  def toShort(o: Any): Short = {
    o match {
      case s: Short => s
      case n: Number => toShort(n)
      case str: String => toShort(str)
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to short")
    }
  }

  def toShort(s: String): Short = s.trim.toShort

  def toShort(number: Number): Short = number.shortValue

  def toInteger(o: Any): Integer = {
    o match {
      case i: Integer => i
      case b: Boolean => if (b) 1 else 0
      case n: Number => toInt(n)
      case s: String => toInt(s)
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to int")
    }
  }

  def toInt(number: Number): Int = number.intValue

  def toInt(s: String): Int = s.trim.toInt

  def toLong(o: Any): Long = {
    o match {
      case l: Long => l
      case n: Number => toLong(n)
      case s: String => toLong(s)
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to long")
    }
  }

  def toLong(number: Number): Long = number.longValue

  def toLong(s: String): Long = s.trim.toLong

  def toFloat(o: Any): Float = {
    o match {
      case f: Float => f
      case n: Number => toFloat(n)
      case s: String => toFloat(s)
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to float")
    }
  }

  def toFloat(number: Number): Float = number.floatValue

  def toFloat(s: String): Float = s.trim.toFloat

  def toDouble(o: Any): Double = {
    o match {
      case d: Double => d
      case n: Number => toDouble(n)
      case s: String => toDouble(s)
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to double")
    }
  }

  def toDouble(number: Number): Double = number.doubleValue

  def toDouble(s: String): Double = s.trim.toDouble

  def toBigDecimal(o: Any): BigDecimal = {
    o match {
      case n: Number => toBigDecimal(n)
      case other => toBigDecimal(other.toString)
    }
  }

  def toBigDecimal(number: Number): BigDecimal = {
    number match {
      case bd: BigDecimal => bd
      case bi: BigInteger => new BigDecimal(bi)
      case l: java.lang.Long => new BigDecimal(l.longValue())
      case other => new BigDecimal(other.doubleValue())
    }
  }

  def toBigDecimal(s: String): BigDecimal = new BigDecimal(s.trim)

  def toDate(o: Any, timeZone: TimeZone): Date = {
    o match {
      case d: Date => d
      case i: Int => internalToDate(i)
      case l: Long => new Date(toTimeWithLocalTimeZone(l, timeZone))
      case s: String if StringUtils.isNumeric(s) =>
        new Date(toTimeWithLocalTimeZone(s.toLong, timeZone))
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to date")
    }
  }

  def internalToDate(v: Int): Date = {
    val t = v * MILLIS_PER_DAY
    new Date(t - LOCAL_TZ.getOffset(t))
  }

  private def toTimeWithLocalTimeZone(time: Long, timeZone: TimeZone) =
    if (timeZone == null) time
    else time + timeZone.getOffset(time)

  def toTimestamp(o: Any, timeZone: TimeZone): Timestamp = {
    o match {
      case t: Timestamp => t
      case d: Date => new Timestamp(toTimeWithLocalTimeZone(d.getTime, timeZone))
      case jud: java.util.Date => new Timestamp(toTimeWithLocalTimeZone(jud.getTime, timeZone))
      case l: Long => new Timestamp(toTimeWithLocalTimeZone(l, timeZone))
      case i: Int => new Timestamp(toTimeWithLocalTimeZone(i.longValue(), timeZone))
      case s: String if StringUtils.isNumeric(s) =>
        new Timestamp(toTimeWithLocalTimeZone(s.toLong, timeZone))
      case other =>
        throw new UnsupportedOperationException(s"Can't convert ${other.getClass} to timestamp")
    }
  }

  private val MILLIS_PER_DAY = 86400000 // = 24 * 60 * 60 * 1000

  private val LOCAL_TZ = TimeZone.getDefault

  def convert(o: Any, dataType: DataType): Any = {
    if (o == null) {
      return null
    }
    dataType match {
      case StringType => toString(o)
      case ByteType => toByte(o)
      case ShortType => toShort(o)
      case IntegerType => toInteger(o)
      case LongType => toLong(o)
      case FloatType => toFloat(o)
      case DoubleType => toDouble(o)
      case BooleanType => toBoolean(o)
      case DateType => toDate(o, LOCAL_TZ)
      case TimestampType => toTimestamp(o, LOCAL_TZ)
      case DecimalType.SYSTEM_DEFAULT | DecimalType.USER_DEFAULT => toBigDecimal(o)
      case other => throw new UnsupportedOperationException(s"${other} not supported")
    }
  }

}
