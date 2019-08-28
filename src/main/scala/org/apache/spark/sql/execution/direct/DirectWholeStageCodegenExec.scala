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

import java.util.concurrent.Callable

import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.{BufferedRowIterator, CodegenSupport, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

case class DirectInputAdapter(child: DirectPlan) extends UnaryDirectExecNode {
  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): Iterator[InternalRow] = {
    child.execute()
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "")

  }
}
case class DirectWholeStageCodegenExec(original: WholeStageCodegenExec)
    extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = original.output

  override def child: DirectPlan = DirectPlanConverter.convertToDirectPlan(original.child)

  val inputDirectPlans: Seq[DirectPlan] = {

    var nextPlan: SparkPlan = original.child
    var children: Seq[SparkPlan] = Seq()
    do {
      children = nextPlan.children
      if (children.size == 1) {
        nextPlan = children.head
      }
    } while (children.size == 1 && nextPlan.isInstanceOf[CodegenSupport]
      && nextPlan.asInstanceOf[CodegenSupport].supportCodegen)

    assert(
      children.size == 1 || children.size == 2,
      "the last plan with CodegenSupport must have one or two input plan ")

    val inputSparkPlans: Seq[SparkPlan] = children.size match {
      case 1 => Seq(nextPlan)
      case 2 => children
    }
    inputSparkPlans.map(DirectPlanConverter.convertToDirectPlan)
  }

  def codegenStageId: Int = original.codegenStageId

  private val (ctx, cleanedSource) = original.doCodeGen()

  override protected def doExecute(): Iterator[InternalRow] = {
    // try to compile and fallback if it failed
    val (_, maxCodeSize) = try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case NonFatal(_) if !Utils.isTesting && sqlContext.conf.codegenFallback =>
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for plan (id=$codegenStageId):\n $treeString")
        return child.execute()
    }
    // Check if compiled code has a too large function
    if (maxCodeSize > sqlContext.conf.hugeMethodLimit) {
      logInfo(
        s"Found too long generated codes and JIT optimization might not work: " +
          s"the bytecode size ($maxCodeSize) is above the limit " +
          s"${sqlContext.conf.hugeMethodLimit}, and the whole-stage codegen was disabled " +
          s"for this plan (id=$codegenStageId). To avoid this, you can raise the limit " +
          s"`${SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key}`:\n$treeString")
      return child.execute()
    }

    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime", DirectSQLMetrics.createTimingMetric())

    // we need convert input rdd to DirectPlan here
    if (inputDirectPlans.length == 1) {
      val inputDirectPlan = inputDirectPlans.head
      val iter = inputDirectPlan.execute()
      val (clazz, _) = CodeGenerator.compile(cleanedSource)
//      var tc = new TestClass()
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(0, Array(iter))
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val v = buffer.hasNext
          if (!v) durationMs += buffer.durationMs()
          v
        }
        override def next: InternalRow = buffer.next()
      }
    } else {
      val leftDirectPlan = inputDirectPlans.head
      val rightDirectPlan = inputDirectPlans(1)
      val leftIter = leftDirectPlan.execute()
      val rightIter = rightDirectPlan.execute()
      val (clazz, _) = CodeGenerator.compile(cleanedSource)
//      var tc = new TestClass()
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(0, Array(leftIter, rightIter))
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val v = buffer.hasNext
          if (!v) durationMs += buffer.durationMs()
          v
        }
        override def next: InternalRow = buffer.next()
      }
    }
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, s"*($codegenStageId) ")
  }

}
