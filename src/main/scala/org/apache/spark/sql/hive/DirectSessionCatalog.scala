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

package org.apache.spark.sql.hive

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchDatabaseException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf

class DirectSessionCatalog(
    externalCatalogBuilder: () => ExternalCatalog,
    globalTempViewManagerBuilder: () => GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
    extends SessionCatalog(
      externalCatalogBuilder,
      globalTempViewManagerBuilder,
      functionRegistry,
      conf,
      hadoopConf,
      parser,
      functionResourceLoader) {

  private val directTempViews = mutable.Map[String, mutable.Map[String, LogicalPlan]]()

  /**
   * Constructs a [[Expression]] based on the provided class that represents a function.
   *
   * This performs reflection to decide what type of [[Expression]] to return in the builder.
   */
  override def makeFunctionExpression(
      name: String,
      clazz: Class[_],
      input: Seq[Expression]): Expression = {

    var udfExpr: Option[Expression] = None
    try {
      // When we instantiate hive UDF wrapper class, we may throw exception if the input
      // expressions don't satisfy the hive UDF, such as type mismatch, input number
      // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
      if (classOf[UDF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[UDAF].isAssignableFrom(clazz)) {
        udfExpr = Some(
          HiveUDAFFunction(
            name,
            new HiveFunctionWrapper(clazz.getName),
            input,
            isUDAFBridgeRequired = true))
        udfExpr.get.dataType // Force it to check input data types.
      } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
        udfExpr = Some(HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), input))
        udfExpr.get.asInstanceOf[HiveGenericUDTF].elementSchema // Force it to check data types.
      } else {
        udfExpr = Some(super.makeFunctionExpression(name, clazz, input))
      }
    } catch {
      case NonFatal(e) =>
        val noHandlerMsg = s"No handler for UDF/UDAF/UDTF '${clazz.getCanonicalName}': $e"
        val errorMsg =
          if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
            s"$noHandlerMsg\nPlease make sure your function overrides " +
              "`public StructObjectInspector initialize(ObjectInspector[] args)`."
          } else {
            noHandlerMsg
          }
        val analysisException = new AnalysisException(errorMsg)
        analysisException.setStackTrace(e.getStackTrace)
        throw analysisException
    }
    udfExpr.getOrElse {
      throw new AnalysisException(s"No handler for UDF/UDAF/UDTF '${clazz.getCanonicalName}'")
    }
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    synchronized {
      super.createDatabase(dbDefinition, ignoreIfExists)
      val db = formatDatabaseName(dbDefinition.name)
      if (!directTempViews.contains(db)) {
        directTempViews.put(db, mutable.Map[String, LogicalPlan]())
      }
    }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    synchronized {
      super.dropDatabase(db, ignoreIfNotExists, cascade)
      val dbName = formatDatabaseName(db)
      directTempViews.remove(dbName)
    }

  override def createTempView(
      name: String,
      tableDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    val identifier = parser.parseTableIdentifier(name)
    if (identifier.database.isEmpty) {
      super.createTempView(identifier.table, tableDefinition, overrideIfExists)
    } else {
      val db = formatDatabaseName(identifier.database.get)
      val table = formatTableName(identifier.table)
      if (!directTempViews.contains(db)) {
        directTempViews.put(db, mutable.Map[String, LogicalPlan]())
      }
      directTempViews(db).put(table, tableDefinition)
    }
  }

  override def dropTempView(name: String): Boolean = synchronized {
    val identifier = parser.parseTableIdentifier(name)
    if (identifier.database.isEmpty) {
      super.dropTempView(name)
    } else {
      val db = formatDatabaseName(identifier.database.get)
      val table = formatTableName(identifier.table)
      requireDbExists(db)
      directTempViews(db).remove(table).isDefined
    }
  }

  override def lookupRelation(name: TableIdentifier): LogicalPlan =
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      if (currentDb.equals(db) || globalTempViewManager.database.equals(db)) {
        super.lookupRelation(name)
      } else {
        val table = formatTableName(name.table)
        if (directTempViews.contains(db) && directTempViews(db).contains(table)) {
          return SubqueryAlias(table, db, directTempViews(db)(table))
        }
        super.lookupRelation(name)
      }
    }

  override def isTemporaryTable(name: TableIdentifier): Boolean = synchronized {
    val isTemp = super.isTemporaryTable(name)
    if (!isTemp && name.database.isDefined) {
      val db = formatDatabaseName(name.database.get)
      val table = formatTableName(name.table)
      if (directTempViews.contains(db) && directTempViews(db).contains(table)) {
        return true
      }
    }
    isTemp
  }

  override def listTables(db: String, pattern: String): Seq[TableIdentifier] = synchronized {
    val directTempViewNames = directTempViews.flatMap {
      case (dbName, tb) =>
        tb.map {
          case (tableName, _) => s"${dbName}.${tableName}"
        }
    }.toSeq

    super.listTables(db, pattern) ++ StringUtils
      .filterPattern(directTempViewNames, pattern)
      .map { name =>
        {
          val Array(dbName, tableName) = name.split("\\.")
          TableIdentifier(tableName, Some(dbName))
        }
      }
  }

  private def requireDbExists(db: String): Unit = {
    if (!directTempViews.contains(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }
}
