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

package org.apache.spark.examples.sql.direct

import org.junit.{After, Before, Test}

import org.apache.spark.examples.sql.TestBase

class DirectExecSuite extends TestBase {

  @Before
  def before(): Unit = {
    spark
      .createDataFrame(Seq(("a", 2, 0), ("bbb", 2, 1), ("c", 3, 0), ("ddd", 4, 1), ("e", 5, 1)))
      .toDF("name", "age", "genda")
      .createOrReplaceTempView("people")
    spark
      .createDataFrame(List(("a", 1, 0), ("b", 2, 1), ("c", 3, 0)))
      .toDF("name", "age", "genda")
      .createOrReplaceTempView("people2")
  }

  @After
  def after(): Unit = {
    spark.close()
  }

  @Test
  def testGenerate(): Unit = {
    assertEquals(
      """
        |select
        |age, str
        |from people
        |LATERAL VIEW
        |explode(split(name, '')) mm
        |as str
        |""".stripMargin,
      true)
  }

  @Test
  def testAgg(): Unit = {
    assertEquals(
      """
        |select
        |genda, count(1)
        |from
        |people group by genda
        |""".stripMargin,
      true)
  }

  @Test
  def testJoin(): Unit = {
    assertEquals("""
        |select
        |* from people t1
        |join people2 t2
        |on t1.name = t2.name
        |""".stripMargin)
  }

  @Test
  def testWindow(): Unit = {
    assertEquals(
      """
        |SELECT
        |name,ROW_NUMBER() OVER (PARTITION BY genda ORDER BY name) as row
        |FROM people
        |""".stripMargin)
  }

}
