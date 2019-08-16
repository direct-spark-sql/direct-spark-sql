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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.direct.{DirectPlan, DirectSQLMetrics, UnaryDirectExecNode}

/**
 * For lazy computing, be sure the generator.terminate() called in the very last
 * TODO reusing the CompletionIterator?
 */
private[direct] sealed case class LazyIterator(func: () => TraversableOnce[InternalRow])
    extends Iterator[InternalRow] {

  lazy val results: Iterator[InternalRow] = func().toIterator
  override def hasNext: Boolean = results.hasNext
  override def next(): InternalRow = results.next()
}

/**
 * Applies a [[Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 *
 * This operator supports whole stage code generation for generators that do not implement
 * terminate().
 *
 * @param generator the generator expression
 * @param requiredChildOutput required attributes from child's output
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty.
 * @param generatorOutput the qualified output attributes of the generator of this node, which
 *                        constructed in analysis phase, and we can not change it, as the
 *                        parent node bound with it already.
 */
case class GenerateDirectExec(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: DirectPlan)
    extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  lazy val boundGenerator: Generator = BindReferences.bindReference(generator, child.output)

  override def doExecute(): Iterator[InternalRow] = {
    // boundGenerator.terminate() should be triggered after all of the rows in the partition
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
    val iter = child.execute()
    val generatorNullRow = new GenericInternalRow(generator.elementSchema.length)
    val rows = if (requiredChildOutput.nonEmpty) {

      val pruneChildForResult: InternalRow => InternalRow =
        if (child.outputSet == AttributeSet(requiredChildOutput)) {
          identity
        } else {
          UnsafeProjection.create(requiredChildOutput, child.output)
        }

      val joinedRow = new JoinedRow
      iter.flatMap { row =>
        // we should always set the left (required child output)
        joinedRow.withLeft(pruneChildForResult(row))
        val outputRows = boundGenerator.eval(row)
        if (outer && outputRows.isEmpty) {
          joinedRow.withRight(generatorNullRow) :: Nil
        } else {
          outputRows.map(joinedRow.withRight)
        }
      } ++ LazyIterator(() => boundGenerator.terminate()).map { row =>
        // we leave the left side as the last element of its child output
        // keep it the same as Hive does
        joinedRow.withRight(row)
      }
    } else {
      iter.flatMap { row =>
        val outputRows = boundGenerator.eval(row)
        if (outer && outputRows.isEmpty) {
          Seq(generatorNullRow)
        } else {
          outputRows
        }
      } ++ LazyIterator(() => boundGenerator.terminate())
    }

    // Convert the rows to unsafe rows.
    val proj = UnsafeProjection.create(output, output)
    proj.initialize(0)
    rows.map { r =>
      numOutputRows += 1
      proj(r)
    }
  }

}
