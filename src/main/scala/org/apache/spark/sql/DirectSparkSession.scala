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

import java.util.Properties

import scala.reflect.runtime.{universe => ru}
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext, TaskContext, TaskContextImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.{setActiveSession, setDefaultSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project, SubqueryAlias}
import org.apache.spark.sql.execution.direct.{
  DirectExecutionContext,
  DirectPlanConverter,
  DirectPlanStrategies
}
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SharedState}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.Utils

class DirectSparkSession private (
    sparkContext: SparkContext,
    private val directExistingSharedState: Option[SharedState])
    extends SparkSession(sparkContext) {
  self =>

  private[sql] def this(sparkContext: SparkContext) = this(sparkContext, None)

  private val DIRECT_SESSION_STATE_BUILDER_CLASS_NAME =
    "org.apache.spark.sql.execution.direct.DirectSessionStateBuilder"

  override lazy val sharedState: SharedState = {
    directExistingSharedState.getOrElse(new SharedState(sparkContext))
  }

  override lazy val sessionState: SessionState = {
    try {
      // invoke `new [Hive]SessionStateBuilder(SparkSession, Option[SessionState])`
      val clazz = Utils.classForName(DIRECT_SESSION_STATE_BUILDER_CLASS_NAME)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(self, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Error while instantiating '$DIRECT_SESSION_STATE_BUILDER_CLASS_NAME':",
          e)
    }
  }

  /**
   * Start a new session with isolated SQL configurations, temporary tables, registered
   * functions are isolated, but sharing the underlying `SparkContext` and cached data.
   *
   * @note Other than the `SparkContext`, all shared state is initialized lazily.
   *       This method will force the initialization of the shared state to ensure that parent
   *       and child sessions are set up with the same shared state. If the underlying catalog
   *       implementation is Hive, this will initialize the metastore, which may take some time.
   * @since 2.0.0
   */
  override def newSession(): DirectSparkSession = {
    val session = new DirectSparkSession(sparkContext, Some(sharedState))
    DirectPlanStrategies.strategies.foreach(strategy =>
      session.extensions.injectPlannerStrategy(_ => strategy))
    session
  }

  def sqlDirectly(sqlText: String): DirectDataTable = {
    try {
      SparkSession.setActiveSession(this)
      val df = sql(sqlText)
      val dfMirror = ru.runtimeMirror(getClass.getClassLoader).reflect(df)
      val deserializerLazyMethodSymbol =
        ru.typeOf[DataFrame].member(ru.TermName("deserializer")).asMethod
      val deserializerLazyMethodMirror = dfMirror.reflectMethod(deserializerLazyMethodSymbol)
      val deserializer = deserializerLazyMethodMirror().asInstanceOf[Expression]
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)

      // hold current active SparkSession
      DirectExecutionContext.get()
      val directExecutedPlan = DirectPlanConverter.convert(df.queryExecution.sparkPlan)
      val taskMemoryManager = new TaskMemoryManager(
        UnifiedMemoryManager.apply(new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"), 1),
        0)
      // prepare a TaskContext for execution
      TaskContext.setTaskContext(
        new TaskContextImpl(0, 0, 0, 0, 0, taskMemoryManager, new Properties, null))
      val iter = directExecutedPlan.execute()
      val data = iter.map(objProj(_).get(0, null).asInstanceOf[Row]).toArray
      DirectDataTable(df.schema, data)
    } finally {
      DirectExecutionContext.get().markCompleted()
      TaskContext.unset()
      DirectExecutionContext.unset()
    }
  }

  def registerTempView(name: String, table: DirectDataTable): Unit = {
    SparkSession.setActiveSession(this)
    val converter = CatalystTypeConverters.createToCatalystConverter(table.schema)
    val plan = Dataset
      .ofRows(
        self,
        LocalRelation(
          table.schema.toAttributes,
          table.data.map(converter(_).asInstanceOf[InternalRow])))
      .logicalPlan
    sessionState.catalog.createTempView(name, plan, true)
  }

  def tempView(name: String): DirectDataTable = {
    val identifier = sessionState.sqlParser.parseTableIdentifier(name)
    val relation = sessionState.catalog.lookupRelation(identifier)
    val (output, data) = relation match {
      case SubqueryAlias(_, Project(_, LocalRelation(output, data, _))) =>
        (output, data)
      case SubqueryAlias(_, LocalRelation(output, data, _)) =>
        (output, data)
      case other => throw new RuntimeException("unexpected Relation[" + other + "]")
    }
    val schema = StructType(output.map(attr => StructField(attr.name, attr.dataType)))
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    val rows = data.map(converter(_).asInstanceOf[Row])
    DirectDataTable(schema, rows)
  }

}

object DirectSparkSession extends Logging {

  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /**
     * Sets a name for the application, which will be shown in the Spark web UI.
     * If no application name is set, a randomly generated name will be used.
     *
     * @since 2.0.0
     */
    def appName(name: String): Builder = config("spark.app.name", name)

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a list of config options based on the given `SparkConf`.
     *
     * @since 2.0.0
     */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
     * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *
     * @since 2.0.0
     */
    private def master(master: String): Builder = config("spark.master", master)

    /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
    def getOrCreate(): DirectSparkSession = synchronized {
      // fixed options in direct mode
      config("spark.ui.enabled", false)
      config("spark.sql.shuffle.partitions", 1)
      config("spark.default.parallelism", 1)
      config("spark.executor.heartbeatInterval", 60)
      config("spark.sql.codegen.wholeStage", true)
      config("spark.sql.codegen.fallback", false)
      config("spark.sql.autoBroadcastJoinThreshold", 0)
      config("spark.sql.join.preferSortMergeJoin", false)

      config("spark.sql.windowExec.buffer.in.memory.threshold", Integer.MAX_VALUE)
      master("local[1]")

      // Get the session from current thread's active session.
      var session = SparkSession.getActiveSession.orNull
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        if (options.nonEmpty) {
          logWarning("Using an existing SparkSession; some configuration may not take effect.")
        }
        return session.asInstanceOf[DirectSparkSession]
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = SparkSession.getDefaultSession.orNull
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
          if (options.nonEmpty) {
            logWarning("Using an existing SparkSession; some configuration may not take effect.")
          }
          return session.asInstanceOf[DirectSparkSession]
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }

          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`,
          // as it's shared by all sessions.
        }

        session = new DirectSparkSession(sparkContext)
        // since SparkSession didn't expose the extensions,we can't share extensions here
        /**
         * Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules,
         * Optimizer rules, Planning Strategies or a customized parser.
         *
         */
        DirectPlanStrategies.strategies.foreach(strategy =>
          session.extensions.injectPlannerStrategy(_ => strategy))

        options.foreach { case (k, v) => session.initialSessionOptions.put(k, v) }
        setDefaultSession(session)
        setActiveSession(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.clearDefaultSession()
          }
        })
      }

      return session.asInstanceOf[DirectSparkSession]
    }
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): Builder = new Builder

}
