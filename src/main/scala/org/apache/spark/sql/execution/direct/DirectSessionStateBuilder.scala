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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{AliasViewChild, Analyzer, CleanupAliases, EliminateUnions, ResolveCreateNamedStruct, ResolveHigherOrderFunctions, ResolveHints, ResolveInlineTables, ResolveLambdaVariables, ResolveTableValuedFunctions, ResolveTimeZone, SubstituteUnresolvedOrdinals, TimeWindowing, TypeCoercion, UnresolvedRelation, UpdateOuterReferences}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{CurrentDate, CurrentTimestamp, DirectCurrentDate, DirectCurrentTimestamp, DirectFromUnixTime, DirectToUnixTimestamp, DirectUnixTimestamp, FromUnixTime, ToUnixTimestamp, UnixTimestamp}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.hive.DirectSessionCatalog
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}

class DirectSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
    extends BaseSessionStateBuilder(session, parentState) {

  override protected lazy val catalog: SessionCatalog = {
    val catalog = new DirectSessionCatalog(
      () => session.sharedState.externalCatalog,
      () => session.sharedState.globalTempViewManager,
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  override protected def newBuilder: NewBuilder = new DirectSessionStateBuilder(_, _)

  override protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override lazy val batches: Seq[Batch] = Seq(
      Batch(
        "Hints",
        fixedPoint,
        new ResolveHints.ResolveBroadcastHints(conf),
        ResolveHints.ResolveCoalesceHints,
        ResolveHints.RemoveAllHints),
      Batch("Simple Sanity Check", Once, LookupFunctions),
      Batch(
        "Substitution",
        fixedPoint,
        CTESubstitution,
        WindowsSubstitution,
        EliminateUnions,
        new SubstituteUnresolvedOrdinals(conf)),
      Batch(
        "Resolution",
        fixedPoint,
        ResolveTableValuedFunctions ::
          ResolveLazyRelations :: // lazy for direct mode

          ResolveRelations ::
          ResolveReferences ::
          ResolveCreateNamedStruct ::
          ResolveDeserializer ::
          ResolveNewInstance ::
          ResolveUpCast ::
          ResolveGroupingAnalytics ::
          ResolvePivot ::
          ResolveOrdinalInOrderByAndGroupBy ::
          ResolveAggAliasInGroupBy ::
          ResolveMissingReferences ::
          ExtractGenerator ::
          ResolveGenerate ::
          ResolveFunctions ::
          ResolveAliases ::
          ResolveSubquery ::
          ResolveSubqueryColumnAliases ::
          ResolveWindowOrder ::
          ResolveWindowFrame ::
          ResolveNaturalAndUsingJoin ::
          ResolveOutputRelation ::
          ExtractWindowExpressions ::
          GlobalAggregates ::
          ResolveAggregateFunctions ::
          TimeWindowing ::
          ResolveInlineTables(conf) ::
          ResolveHigherOrderFunctions(catalog) ::
          ResolveLambdaVariables(conf) ::
          ResolveTimeZone(conf) ::
          ResolveRandomSeed ::
          TypeCoercion.typeCoercionRules(conf) ++
          extendedResolutionRules: _*),
      Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
      Batch("View", Once, AliasViewChild(conf)),
      Batch("Nondeterministic", Once, PullOutNondeterministic),
      Batch("UDF", Once, HandleNullInputsForUDF),
      Batch("FixNullability", Once, FixNullability),
      Batch("Subquery", Once, UpdateOuterReferences),
      Batch("Cleanup", fixedPoint, CleanupAliases))

    object ResolveLazyRelations extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
        case u: UnresolvedRelation => resolveRelation(u)
      }

      def resolveRelation(plan: LogicalPlan): LogicalPlan = plan match {
        case UnresolvedRelation(ident) if catalog.isTemporaryTable(ident) =>
          val resolvedRelation = ResolveRelations.resolveRelation(plan)
          resolvedRelation match {
            case SubqueryAlias(name, Project(projectList, LocalRelation(output, data, _))) =>
              SubqueryAlias(name, Project(projectList, NamedLocalRelation(output, data, ident)))
            case SubqueryAlias(name, LocalRelation(output, data, _)) =>
              SubqueryAlias(name, NamedLocalRelation(output, data, ident))
            case _ => plan
          }
        case _ => plan
      }
    }
  }

  override protected def optimizer: Optimizer = {
    new SparkOptimizer(catalog, experimentalMethods) {

      override def preOptimizationBatches: Seq[Batch] = {
        Batch("Direct time", Once, DirectTime) :: Nil
      }

      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules

      object DirectTime extends Rule[LogicalPlan] {
        def apply(plan: LogicalPlan): LogicalPlan = {
          plan transformAllExpressions {
            case CurrentDate(Some(timeZoneId)) =>
              DirectCurrentDate(Some(timeZoneId))
            case CurrentTimestamp() => DirectCurrentTimestamp()
            case ToUnixTimestamp(timeExp, format, timeZoneId) =>
              DirectToUnixTimestamp(timeExp, format, timeZoneId)
            case UnixTimestamp(timeExp, format, timeZoneId) =>
              DirectUnixTimestamp(timeExp, format, timeZoneId)
            case FromUnixTime(sec, format, timeZoneId) =>
              DirectFromUnixTime(sec, format, timeZoneId)
          }
        }
      }
    }
  }
}
