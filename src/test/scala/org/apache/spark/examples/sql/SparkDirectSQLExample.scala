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
package org.apache.spark.examples.sql

import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.time.StopWatch

import org.apache.spark.sql.{DirectSparkSession, Row, SparkSession}
import org.apache.spark.sql.types._

object SparkDirectSQLExample {

  // $example on:create_ds$
  case class Person(name: String, age: Long)

  // $example off:create_ds$

  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = DirectSparkSession.builder()
        .getOrCreate()

    // code gen dir prepare
//    val code_gen_path = "target/generated-sources"
//    sys.props(ICookable.SYSTEM_PROPERTY_SOURCE_DEBUGGING_ENABLE) = "true"
//    sys.props(ICookable.SYSTEM_PROPERTY_SOURCE_DEBUGGING_DIR) = code_gen_path

    // For implicit conversions like converting RDDs to DataFrames
    // $example off:init_session$

    runBasicDataFrameExample(spark)
//    //    runDatasetCreationExample(spark)
//    //    runInferSchemaExample(spark)
//    //    runProgrammaticSchemaExample(spark)
//    runSubqueryExample(spark)
//    runGenerateExample(spark)
//    runViewExample(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: DirectSparkSession): Unit = {
    // $example on:create_df$
    val df = spark
      .createDataFrame(List(("a", 2, 0), ("bbb", 2, 1), ("c", 3, 0), ("ddd", 4, 1), ("e", 5, 1)))
      .toDF("name", "age", "genda")
    val df2 = spark
      .createDataFrame(List(("a", 1, 0), ("b", 2, 1), ("c", 3, 0)))
      .toDF("name", "age", "genda")
    df.createOrReplaceTempView("people")
    df2.createOrReplaceTempView("people2")

//    val sqlDF = spark.sqlDirectly("SELECT substring(name,0,1) as c1 ,age as c2 FROM people where age>1")
    val sqlDF = spark.sqlDirectly("SELECT substring(t1.name,0,1) as c1 ,substring(t2.name,0,2) as c2 FROM people t1 left outer join people2 t2 on t1.name=t2.name and t2.age>0")

    //    val rt = sqlDF.collect()
    val s1 = StopWatch.createStarted()
//    val sqlDF = spark.sqlDirectly("SELECT genda,max(age),count(*) FROM people group by genda")
//    val sqlDF = spark.sqlDirectly("SELECT name,ROW_NUMBER() OVER (PARTITION BY genda ORDER BY name) as row FROM people")
    val rt = sqlDF.data
    s1.stop()
    println("s1:" + s1.getTime(TimeUnit.MILLISECONDS))
    println(rt.mkString(","))

    spark.registerTempView("t1", sqlDF)

    val res = spark.sqlDirectly("select * from t1")
    val ts1 = spark.catalog.listTables().collect()

    val spark1 = spark.newSession()
    val ts2 = spark1.catalog.listTables().collect()
    println("end")
    //    val s2 = StopWatch.createStarted()
//    val rt2 = sqlDF.collectDirectly()
//    s2.stop()
//    println("s2:" + s2.getTime(TimeUnit.MILLISECONDS))
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:run_sql$

    // $example on:global_temp_view$
    // Register the DataFrame as a global temporary view
    //    df.createGlobalTempView("people")
    //
    //    // Global temporary view is tied to a system preserved database `global_temp`
    //    spark.sql("SELECT * FROM global_temp.people").show()
    //    // +----+-------+
    //    // | age|   name|
    //    // +----+-------+
    //    // |null|Michael|
    //    // |  30|   Andy|
    //    // |  19| Justin|
    //    // +----+-------+
    //
    //    // Global temporary view is cross-session
    //    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:global_temp_view$
  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:create_ds$
    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_ds$
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    // $example on:schema_inferring$
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
    // $example off:schema_inferring$
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString
      .split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
    // $example off:programmatic_schema$
  }
  private def runSubqueryExample(spark: DirectSparkSession): Unit = {
    // $example on:subquery$
    val df = spark
        .createDataFrame(List(("a", 2, 0), ("bbb", 2, 1), ("c", 3, 0), ("ddd", 4, 1), ("e", 5, 1)))
        .toDF("name", "age", "genda")

    df.createOrReplaceTempView("people")

    //    val rt = sqlDF.collect()
    val s1 = StopWatch.createStarted()
    val sqlDF = spark.sqlDirectly("""
                                    |select sum(age), sum(genda) from people where age < (
                                    |select max(age) from people
                                    |)
                                    |""".stripMargin)
    val rt = sqlDF.data
    s1.stop()
    // scalastyle:off println
    println("s1:" + s1.getTime(TimeUnit.MILLISECONDS))
    println(rt.mkString(","))
    // scalastyle:off println
    // $example off:subquery$
  }


  private def runGenerateExample(spark: DirectSparkSession): Unit = {
    // $example on:subquery$
    val df = spark
      .createDataFrame(List(("a", 2, 0), ("bbb", 2, 1), ("c", 3, 0), ("ddd", 4, 1), ("e", 5, 1)))
      .toDF("name", "age", "genda")

    df.createOrReplaceTempView("people")

    //    val rt = sqlDF.collect()
    val s1 = StopWatch.createStarted()

    val sqlDF = spark.sqlDirectly(
      """
        |select
        |age, str
        |from people
        |LATERAL VIEW
        |explode(split(name, '')) mm
        |as str
        |
        |""".stripMargin)
    val rt = sqlDF.data
    s1.stop()
    // scalastyle:off println
    println("s1:" + s1.getTime(TimeUnit.MILLISECONDS))
    println(rt.mkString(","))
    // scalastyle:off println
    // $example off:subquery$
  }

  def runViewExample(spark: DirectSparkSession): Unit = {
    // $example on:view$
    val df = spark
      .createDataFrame(List(("a", 2, 0), ("bbb", 2, 1), ("c", 3, 0), ("ddd", 4, 1), ("e", 5, 1)))
      .toDF("name", "age", "genda")
    val table = spark.sqlDirectly("select * from people")
    spark.registerTempView("mm.test", table)
    val table1 = spark.sqlDirectly(
      """
        |select  genda, count(1) as cnt from mm.test group by genda
        |""".stripMargin)
    println(table1.data.mkString(","))
    // $example off:view$

  }
}
