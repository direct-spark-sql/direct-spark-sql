# direct-spark-sql
a hyper-optimized single-node(local) version of spark sql engine, which's fundamental data structure  is scala Iterator rather than RDD.

##Usage
see [DirectExecSuite.scala](https://github.com/direct-spark-sql/direct-spark-sql/blob/master/src/test/scala/org/apache/spark/examples/sql/direct/DirectExecSuite.scala)
```
val spark = DirectSparkSession.builder().getOrCreate()
  spark
      .createDataFrame(Seq(("a", 2, 0), ("bbb", 2, 1), ("c", 3, 0), ("ddd", 4, 1), ("e", 5, 1)))
      .toDF("name", "age", "genda")
      .createOrReplaceTempView("people")
    spark
      .createDataFrame(List(("a", 1, 0), ("b", 2, 1), ("c", 3, 0)))
      .toDF("name", "age", "genda")
      .createOrReplaceTempView("people2")
      
 val resultTable = spark.sqlDirectly("""
                                             |select
                                             |* from people t1
                                             |join people2 t2
                                             |on t1.name = t2.name
                                             |""".stripMargin)


```

## Contributing
It's under active developing now , may be released in one or two months.

Please run follow command before submitting a PR:
```$xslt
 mvn clean install -Pcheck-style
```
