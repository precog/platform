/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package daze

import annotation.tailrec

import com.precog.common._
import com.precog.common.security._
import com.precog.bytecode._
import com.precog.yggdrasil._
import com.precog.yggdrasil.TableModule._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table.ColumnarTableModuleConfig
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.util.IdSourceConfig
import com.precog.util._

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._
import collection.immutable.ListSet

import org.slf4j.LoggerFactory

import akka.dispatch.Future

import scalaz.{NonEmptyList => NEL, _}
import scalaz.StateT.{StateMonadTrans, stateTMonadState}
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.partialFunction._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import scala.Function._
import scala.collection.immutable.Queue

trait EvaluatorConfig extends IdSourceConfig {
  def maxSliceSize: Int
}

case class EvaluationContext(apiKey: APIKey, basePath: Path, startTime: DateTime)

trait EvaluatorModule[M[+_]] extends CrossOrdering
    with Memoizer
    with TypeInferencer
    with CondRewriter
    with JoinOptimizer
    with SortPushDown
    with OpFinderModule[M] 
    with StaticInlinerModule[M] 
    with ReductionFinderModule[M]
    with TransSpecableModule[M]
    with PredicatePullupsModule[M]
    with TableModule[M] // Remove this explicit dep!
    with TableLibModule[M] {

  import dag._
  import instructions._

  type GroupId = Int
  
  type Evaluator[N[+_]] <: EvaluatorLike[N]
  
  abstract class EvaluatorLike[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M)
      extends OpFinder
      with ReductionFinder
      with StaticInliner
      with PredicatePullups
      with YggConfigComponent {
    
    type YggConfig <: EvaluatorConfig

    import library._  
    import yggConfig._
    import trans._
    import constants._

    private implicit val N = N0
    private val evalLogger = LoggerFactory.getLogger("com.precog.daze.Evaluator")
    private val transState = StateMonadTrans[EvaluatorState]
    private val monadState = stateTMonadState[EvaluatorState, N]

    def report: QueryLogger[N, instructions.Line]

    private def MorphLogger(loc: instructions.Line): MorphLogger = new MorphLogger {
      def info(msg: String): M[Unit] = nm(report.info(loc, msg))
      def warn(msg: String): M[Unit] = nm(report.warn(loc, msg))
      def error(msg: String): M[Unit] = nm(report.error(loc, msg))
      def die(): M[Unit] = nm(report.die())
    }

    def MorphContext(ctx: EvaluationContext, node: DepGraph): MorphContext =
      new MorphContext(ctx, MorphLogger(node.loc))
  
    def freshIdScanner: Scanner
    
    def Forall: Reduction { type Result = Option[Boolean] }
    def Exists: Reduction { type Result = Option[Boolean] }
    def concatString(ctx: MorphContext): F2
    def coerceToDouble(ctx: MorphContext): F1

    def composeOptimizations(optimize: Boolean, funcs: List[DepGraph => DepGraph]): DepGraph => DepGraph =
      if (optimize) funcs.reverse.map(Endo[DepGraph]).suml.run else identity

    // Have to be idempotent on subgraphs
    def stagedRewriteDAG(optimize: Boolean, ctx: EvaluationContext): DepGraph => DepGraph = {
      composeOptimizations(optimize, List(
        inlineStatics(_, ctx),
        optimizeJoins(_),
        rewriteConditionals(_)
      ))
    }

    def fullRewriteDAG(optimize: Boolean, ctx: EvaluationContext): DepGraph => DepGraph = {
      stagedRewriteDAG(optimize, ctx) andThen
        (orderCrosses _) andThen
        composeOptimizations(optimize, List[DepGraph => DepGraph](
          // TODO: Predicate pullups break a SnapEngage query (see PLATFORM-951)
          //predicatePullups(_, ctx),
          inferTypes(JType.JUniverseT),
          { g => megaReduce(g, findReductions(g, ctx)) },
          pushDownSorts,
          memoize
        ))
    }

    /**
     * The entry point to the evaluator.  The main implementation of the evaluator
     * is comprised by the inner functions, `fullEval` (the main evaluator function)
     * and `prepareEval` (which has the primary eval loop).
     */
    def eval(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean): N[Table] = {
      evalLogger.debug("Eval for {} = {}", ctx.apiKey.toString, graph)

      val rewrittenDAG = fullRewriteDAG(optimize, ctx)(graph)

      def resolveTopLevelGroup(spec: BucketSpec, splits: Map[Identifier, Int => N[Table]]): StateT[N, EvaluatorState, N[GroupingSpec]] = spec match {
        case UnionBucketSpec(left, right) => 
          for {
            leftSpec <- resolveTopLevelGroup(left, splits) 
            rightSpec <- resolveTopLevelGroup(right, splits)
            
            val commonIds = findCommonIds(left, right)
            val keySpec = buildKeySpec(commonIds)
          } yield {
            for {
              leftRes <- leftSpec
              rightRes <- rightSpec
            } yield GroupingAlignment(keySpec, keySpec, leftRes, rightRes, GroupingSpec.Union)
          }
        
        case IntersectBucketSpec(left, right) => 
          for {
            leftSpec <- resolveTopLevelGroup(left, splits)
            rightSpec <- resolveTopLevelGroup(right, splits)
            
            val commonIds = findCommonIds(left, right)
            val keySpec = buildKeySpec(commonIds)
          } yield {
            for {
              leftRes <- leftSpec
              rightRes <- rightSpec
            } yield GroupingAlignment(keySpec, keySpec, leftRes, rightRes, GroupingSpec.Intersection)
          }
        
        case dag.Group(id, target, forest) => 
          val common = findCommonality(enumerateGraphs(forest) + target)
          
          common match {
            case Some(reducedTarget) => 
              for {
                pendingTarget <- prepareEval(reducedTarget, splits)
                resultTargetTable = pendingTarget.table.transform { liftToValues(pendingTarget.trans) }
                _ <- monadState.gets(identity)
                subSpec <- resolveLowLevelGroup(resultTargetTable, reducedTarget, forest, splits)
              } yield {
                // TODO FIXME if the target has forcing points, targetTrans is insufficient
                val Some(trans) = mkTransSpec(target, reducedTarget, ctx)
                N.point(GroupingSource(resultTargetTable, SourceKey.Single, Some(liftToValues(trans)), id, subSpec))
              }
              
            case None => 
              sys.error("Attempted to evaluate bucket spec without transspecable commonality.")
          }
        
        case UnfixedSolution(_, _) | dag.Extra(_) => 
          sys.error("assertion error")
      }

      //** only used in resolveTopLevelGroup **/
      def resolveLowLevelGroup(commonTable: Table, commonGraph: DepGraph, forest: BucketSpec, splits: Map[Identifier, Int => N[Table]]): StateT[N, EvaluatorState, GroupKeySpec] = forest match {
        case UnionBucketSpec(left, right) =>
          for {
            leftRes <- resolveLowLevelGroup(commonTable, commonGraph, left, splits)
            rightRes <- resolveLowLevelGroup(commonTable, commonGraph, right, splits)
          } yield GroupKeySpecOr(leftRes, rightRes)
        
        case IntersectBucketSpec(left, right) => 
          for {
            leftRes <- resolveLowLevelGroup(commonTable, commonGraph, left, splits)
            rightRes <- resolveLowLevelGroup(commonTable, commonGraph, right, splits)
          } yield GroupKeySpecAnd(leftRes, rightRes)
        
        case UnfixedSolution(id, solution) =>
          val Some(spec) = mkTransSpec(solution, commonGraph, ctx)
          for {
            _ <- monadState.gets(identity)
            liftedTrans = TransSpec.deepMap(spec) {
              case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
            }
          } yield GroupKeySpecSource(CPathField(id.toString), liftedTrans)
        
        case dag.Extra(graph) =>
          val Some(spec) = mkTransSpec(graph, commonGraph, ctx)
          for {
            state <- monadState.gets(identity)
            extraId = state.extraCount
            _ <- monadState.modify { _.copy(extraCount = extraId + 1) }

            liftedTrans = TransSpec.deepMap(spec) {
              case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
            }
          } yield GroupKeySpecSource(CPathField("extra" + extraId), trans.Filter(liftedTrans, liftedTrans))
        
        case dag.Group(_, _, _) => 
          sys.error("assertion error")
      }

      def prepareEval(graph: DepGraph, splits: Map[Identifier, Int => N[Table]]): StateT[N, EvaluatorState, PendingTable] = {
        evalLogger.trace("Loop on %s".format(graph))

        val startTime = System.nanoTime
        
        def assumptionCheck(graph: DepGraph): StateT[N, EvaluatorState, Option[Table]] =
          for (state <- monadState.gets(identity)) yield state.assume.get(graph)
        
        def memoized(graph: DepGraph, f: DepGraph => StateT[N, EvaluatorState, PendingTable]) = {
          def memoizedResult = graph match {
            case graph: StagingPoint => {
              for {
                pending <- f(graph)
                _ <- monadState.modify { state => state.copy(assume = state.assume + (graph -> pending.table)) }
              } yield pending
            }
            
            case _ => f(graph)
          }
  
          assumptionCheck(graph) flatMap { assumedResult: Option[Table] =>
            val liftedAssumption = assumedResult map { case table =>
              monadState point PendingTable(table, graph, TransSpec1.Id)
            }
            
            liftedAssumption getOrElse memoizedResult
          }
        }
        
        def get0(pt: PendingTable): (TransSpec1, DepGraph) = (pt.trans, pt.graph)
        
        def set0(pt: PendingTable, tg: (TransSpec1, DepGraph)): StateT[N, EvaluatorState, PendingTable] =
          for {
            _ <- monadState.modify { state => state.copy(assume = state.assume + (tg._2 -> pt.table)) }
          } yield pt.copy(trans = tg._1, graph = tg._2)

        def init0(tg: (TransSpec1, DepGraph)): StateT[N, EvaluatorState, PendingTable] =
          memoized(tg._2, evalNotTransSpecable)
        
        type TSM[+T] = StateT[N, EvaluatorState, T]
        def evalTransSpecable(to: DepGraph): StateT[N, EvaluatorState, PendingTable] =
          mkTransSpecWithState[TSM, PendingTable](to, None, ctx, get0, set0, init0)
        
        def evalNotTransSpecable(graph: DepGraph): StateT[N, EvaluatorState, PendingTable] = graph match {
          case dag.Observe(data, samples) =>
            for {
              pendingTableData <- prepareEval(data, splits)
              pendingTableSamples <- prepareEval(samples, splits)

              tableData = pendingTableData.table
              tableSamples = pendingTableSamples.table

              result <- {
                //idea: make `samples` table an infinite sample table with replacement
                //then don't need to kick out non-infinite sample sets in the compiler
                //also can make solution more general by accepting discrete random variable
                val keySpec = trans.WrapObject(trans.DerefObjectStatic(TransSpec1.Id, paths.Key), paths.Key.name)
                val valueSpec = trans.WrapObject(trans.DerefObjectStatic(TransSpec1.Id, paths.Value), paths.Value.name)
                
                val keyTable = tableData.transform(keySpec).canonicalize(maxSliceSize)
                val valueTable = tableSamples.transform(valueSpec).canonicalize(maxSliceSize)

                transState liftM mn(keyTable.zip(valueTable) flatMap { _.sort(keySpec, SortAscending) })
              }
            } yield {
              PendingTable(result, graph, TransSpec1.Id) // TODO: is this sorted?
            }

          case Join(op, joinSort @ (IdentitySort | ValueSort(_)), left, right) => 
            // TODO binary typing

            for {
              pendingTableLeft <- prepareEval(left, splits)
              pendingTableRight <- prepareEval(right, splits)
            } yield {
              assert(pendingTableLeft.graph != pendingTableRight.graph, "TransSpecable case should have already been handled")
              
              (left.identities, right.identities) match {
                case (Identities.Specs(_), Identities.Specs(_)) =>
                  val prefixLength = sharedPrefixLength(left, right)

                  val identities = left.identities.length + right.identities.length - prefixLength
                  val key = joinSort match {
                    case IdentitySort => buildJoinKeySpec(prefixLength)
                    case ValueSort(id) => trans.DerefObjectStatic(Leaf(Source), CPathField("sort-" + id))
                    case _ => sys.error("unreachable code")
                  }

                  val spec = buildWrappedJoinSpec(prefixLength, left.identities.length, right.identities.length)(transFromBinOp(op, MorphContext(ctx, graph)))

                  val leftResult = pendingTableLeft.table.transform(liftToValues(pendingTableLeft.trans))
                  val rightResult = pendingTableRight.table.transform(liftToValues(pendingTableRight.trans))
                  val result = join(leftResult, rightResult)(key, spec)

                  PendingTable(result, graph, TransSpec1.Id)

                case (Identities.Undefined, _) | (_, Identities.Undefined) =>
                  PendingTable(Table.empty, graph, TransSpec1.Id)
              }
            }
          
          case dag.Filter(joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => 
            // TODO binary typing
            for {
              pendingTableTarget <- prepareEval(target, splits)
              pendingTableBoolean <- prepareEval(boolean, splits)
            } yield {
              assert(pendingTableTarget.graph != pendingTableBoolean.graph, "TransSpecable case should have already been handled")

              val prefixLength = sharedPrefixLength(target, boolean)
              
              val key = joinSort match {
                case IdentitySort => buildJoinKeySpec(prefixLength)
                case ValueSort(id) => trans.DerefObjectStatic(Leaf(Source), CPathField("sort-" + id))
                case _ => sys.error("unreachable code")
              }
              
              val spec = buildWrappedJoinSpec(prefixLength, target.identities.length, boolean.identities.length) { (srcLeft, srcRight) =>
                trans.Filter(srcLeft, srcRight)
              }
              val parentTargetTable = pendingTableTarget.table
              val targetResult = parentTargetTable.transform(liftToValues(pendingTableTarget.trans))
              val parentBooleanTable = pendingTableBoolean.table
              val booleanResult = parentBooleanTable.transform(liftToValues(pendingTableBoolean.trans))
              val result = join(targetResult, booleanResult)(key, spec)

              PendingTable(result, graph, TransSpec1.Id)
            }
          
          case s: SplitParam => 
            sys.error("Inlining of SplitParam failed")
          
          // not using extractors due to bug
          case s: SplitGroup => 
            val f = splits(s.parentId)
            transState liftM f(s.id) map { PendingTable(_, graph, TransSpec1.Id) }
          
          case Const(value) =>
            val table = value match {
              case CString(str) => Table.constString(Set(str))
              
              case CLong(ln) => Table.constLong(Set(ln))
              case CDouble(d) => Table.constDouble(Set(d))
              case CNum(n) => Table.constDecimal(Set(n))
              
              case CBoolean(b) => Table.constBoolean(Set(b))

              case CNull => Table.constNull
              
              case CDate(d) => Table.constDate(Set(d))

              case RObject.empty => Table.constEmptyObject
              case RArray.empty => Table.constEmptyArray
              case CEmptyObject => Table.constEmptyObject
              case CEmptyArray => Table.constEmptyArray

              case CUndefined => Table.empty

              case rv => Table.fromRValues(Stream(rv))
            }

            val spec = buildConstantWrapSpec(Leaf(Source))
            monadState point PendingTable(table.transform(spec), graph, TransSpec1.Id)

          case Undefined() =>
            monadState point PendingTable(Table.empty, graph, TransSpec1.Id)
          
          // TODO technically, we can do this without forcing by pre-lifting PendingTable#trans
          case dag.New(parent) => 
            for {
              pendingTable <- prepareEval(parent, splits)
              idSpec = makeTableTrans(Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))
              
              tableM2 = pendingTable.table.transform(liftToValues(pendingTable.trans)).transform(idSpec)
            } yield PendingTable(tableM2, graph, TransSpec1.Id)
        
          case dag.LoadLocal(parent, jtpe) => 
            for {
              pendingTable <- prepareEval(parent, splits)
              Path(prefixStr) = ctx.basePath
              f1 = concatString(MorphContext(ctx, graph)).applyl(CString(prefixStr.replaceAll("/$", "")))
              trans2 = trans.Map1(trans.DerefObjectStatic(pendingTable.trans, paths.Value), f1)
              back <- transState liftM mn(pendingTable.table.transform(trans2).load(ctx.apiKey, jtpe))
            } yield PendingTable(back, graph, TransSpec1.Id)
          
          case dag.Morph1(mor, parent) => 
            for {
              pendingTable <- prepareEval(parent, splits)
              back <- transState liftM mn(mor(pendingTable.table.transform(liftToValues(pendingTable.trans)), MorphContext(ctx, graph)))
            } yield PendingTable(back, graph, TransSpec1.Id)
        
          case dag.Morph2(mor, left, right) => 
            lazy val spec = trans.InnerArrayConcat(trans.WrapArray(Leaf(SourceLeft)), trans.WrapArray(Leaf(SourceRight)))
            lazy val specRight = trans.InnerArrayConcat(trans.WrapArray(Leaf(SourceRight)), trans.WrapArray(Leaf(SourceLeft)))
            lazy val key = trans.DerefObjectStatic(Leaf(Source), paths.Key)
            
            for {
              pair <- zip(prepareEval(left, splits), prepareEval(right, splits))
              (pendingTableLeft, pendingTableRight) = pair

              leftResult = pendingTableLeft.table.transform(liftToValues(pendingTableLeft.trans))
              rightResult = pendingTableRight.table.transform(liftToValues(pendingTableRight.trans))

              transform = (aligned: Table) => {
                val leftSpec0 = DerefObjectStatic(DerefArrayStatic(TransSpec1.Id, CPathIndex(0)), paths.Value)
                val rightSpec0 = DerefObjectStatic(DerefArrayStatic(TransSpec1.Id, CPathIndex(1)), paths.Value)

                aligned.transform(InnerArrayConcat(trans.WrapArray(leftSpec0), trans.WrapArray(rightSpec0)))
              }
              
              transformedAndMorph1 <- transState liftM (mor.alignment match {
                case MorphismAlignment.Cross(morph1) => 
                  mn(morph1) map { Tuple2(transform(leftResult.cross(rightResult)(spec)), _) }

                case MorphismAlignment.Match(morph1) if sharedPrefixLength(left, right) > 0 => 
                  mn(morph1) map { Tuple2(transform(join(leftResult, rightResult)(key, spec)), _) }

                case MorphismAlignment.Match(morph1) if sharedPrefixLength(left, right) == 0 => 
                  mn(morph1) map { 
                    Tuple2(
                      transform(
                        if (left.isSingleton || !right.isSingleton) {
                          rightResult.cross(leftResult)(specRight) 
                        } else {
                          leftResult.cross(rightResult)(spec) 
                        }
                      ), 
                      _
                    )
                  }

                case MorphismAlignment.Custom(_, f) =>
                  mn(f(leftResult, rightResult))
              })

              (transformed, morph1) = transformedAndMorph1

              back <- transState liftM mn(morph1(transformed, MorphContext(ctx, graph)))
            } yield PendingTable(back, graph, TransSpec1.Id)
        
          case dag.Distinct(parent) =>
            val idSpec = makeTableTrans(Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))

            for {
              pending <- prepareEval(parent, splits)

              valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
              table = pending.table.transform(liftToValues(pending.trans))

              sorted <- transState liftM mn(table.sort(valueSpec, SortAscending))
              distinct = sorted.distinct(valueSpec)
              result = distinct.transform(idSpec)
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }

          /**
          returns an array (to be dereferenced later) containing the result of each reduction
          */
          case m @ MegaReduce(reds, parent) => 
            val firstCoalesce = reds.map {
              case (_, reductions) => coalesce(reductions.map((_, None)))
            }

            val reduction = coalesce(firstCoalesce.zipWithIndex map { case (r, j) => (r, Some(j)) })

            val spec = combineTransSpecs(reds.map(_._1))

            for {
              pendingTable <- prepareEval(parent, splits)
              liftedTrans = liftToValues(pendingTable.trans)
              
              result = mn(pendingTable.table
                .transform(liftedTrans)
                .transform(DerefObjectStatic(Leaf(Source), paths.Value))
                .transform(spec)
                .reduce(reduction.reducer(MorphContext(ctx, graph)))(reduction.monoid))

              table = result.map(reduction.extract)

              keyWrapped = trans.WrapObject(
                trans.ConstLiteral(
                  CEmptyArray,
                  trans.DerefArrayStatic(Leaf(Source), CPathIndex(0))),
                paths.Key.name)

              valueWrapped = trans.InnerObjectConcat(
                keyWrapped,
                trans.WrapObject(Leaf(Source), paths.Value.name))

              wrapped <- transState liftM table map { _.transform(valueWrapped) }
              rvalue <- transState liftM result.map(reduction.extractValue)

              _ <- monadState.modify { state =>
                state.copy(
                  assume = state.assume + (m -> wrapped), //TODO: is this right?
                  reductions = state.reductions + (m -> rvalue)
                )
              }
            } yield {
              PendingTable(wrapped, graph, TransSpec1.Id)
            }

          case r @ dag.Reduce(red, parent) => 
            for {
              pendingTable <- prepareEval(parent, splits)
              liftedTrans = liftToValues(pendingTable.trans)
              result <- transState liftM mn(red(pendingTable.table.transform(DerefObjectStatic(liftedTrans, paths.Value)), MorphContext(ctx, graph)))
              wrapped = result transform buildConstantWrapSpec(Leaf(Source))
            } yield PendingTable(wrapped, graph, TransSpec1.Id)
          
          case s @ dag.Split(spec, child, id) => 
            val idSpec = makeTableTrans(Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))
          
            val params = child.foldDown(true) {
              case param: dag.SplitParam if param.parentId == id => Set(param)
            }

            val table = for {
              grouping <- resolveTopLevelGroup(spec, splits)
              state <- monadState.gets(identity)
              grouping2 <- transState liftM grouping
              result <- transState liftM mn(Table.merge(grouping2) { (key, map) =>
                val splits2 = splits + (id -> (map andThen mn))
                val rewritten = params.foldLeft(child) {
                  case (child, param) =>
                    val subKey = key \ param.id.toString
                    replaceNode(child, param, Const(subKey)(param.loc))
                }

                val back = fullEval(rewritten, splits2, id :: splits.keys.toList)
                back.eval(state)
              })
            } yield {
              result.transform(idSpec)
            }

            table map { PendingTable(_, graph, TransSpec1.Id) }
          
          case dag.Assert(pred, child) => 
            for {
              predPending <- prepareEval(pred, splits)
              childPending <- prepareEval(child, splits)     // TODO squish once brian's PR lands
              
              liftedTrans = liftToValues(predPending.trans)
              predTable = predPending.table transform DerefObjectStatic(liftedTrans, paths.Value)
              
              truthiness <- transState liftM mn(predTable.reduce(Forall reducer MorphContext(ctx, graph))(Forall.monoid))
            
              assertion = if (truthiness getOrElse false) {
                N.point(())
              } else {
                for {
                  _ <- report.error(graph.loc, "Assertion failed")
                  _ <- report.die() // Arrrrrrrgggghhhhhhhhhhhhhh........ *gurgle*
                } yield ()
              }
              _ <- transState liftM assertion
              
              result = childPending.table transform liftToValues(childPending.trans)
            } yield PendingTable(result, graph, TransSpec1.Id)
            
          case c @ dag.Cond(pred, left, _, right, _) =>
            evalNotTransSpecable(c.peer)
          
          case IUI(union, left, right) => 
            for {
              pair <- zip(prepareEval(left, splits), prepareEval(right, splits))
              (leftPending, rightPending) = pair

              keyValueSpec = TransSpec1.PruneToKeyValue

              leftTable = leftPending.table.transform(liftToValues(leftPending.trans))
              rightTable = rightPending.table.transform(liftToValues(rightPending.trans))

              leftSortedM = transState liftM mn(leftTable.sort(keyValueSpec, SortAscending))
              rightSortedM = transState liftM mn(rightTable.sort(keyValueSpec, SortAscending))

              pair <- zip(leftSortedM, rightSortedM)
              (leftSorted, rightSorted) = pair

              result = if (union) {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(Leaf(Source), Leaf(Source), Leaf(SourceLeft))
              } else {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(TransSpec1.DeleteKeyValue, TransSpec1.DeleteKeyValue, TransSpec2.LeftId)
              }
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }
        
          // TODO unify with IUI
          case Diff(left, right) =>
            for {
              pair <- zip(prepareEval(left, splits), prepareEval(right, splits))
              (leftPending, rightPending) = pair

              leftTable = leftPending.table.transform(liftToValues(leftPending.trans))
              rightTable = rightPending.table.transform(liftToValues(rightPending.trans))

              // this transspec prunes everything that is not a key or a value.
              keyValueSpec = TransSpec1.PruneToKeyValue

              leftSortedM = transState liftM mn(leftTable.sort(keyValueSpec, SortAscending))
              rightSortedM = transState liftM mn(rightTable.sort(keyValueSpec, SortAscending))

              pair <- zip(leftSortedM, rightSortedM)
              (leftSorted, rightSorted) = pair

              result = leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(TransSpec1.Id, TransSpec1.DeleteKeyValue, TransSpec2.DeleteKeyValueLeft)
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }
    
          case j @ Join(op, joinSort @ (CrossLeftSort | CrossRightSort), left, right) => 
            val isLeft = joinSort == CrossLeftSort

            for {
              pendingTableLeft <- prepareEval(left, splits)
              pendingTableRight <- prepareEval(right, splits)

              leftResult = pendingTableLeft.table.transform(liftToValues(pendingTableLeft.trans))
              rightResult = pendingTableRight.table.transform(liftToValues(pendingTableRight.trans))

              valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)

              result = if (isLeft) {
                leftResult.paged(maxSliceSize).compact(valueSpec).cross(rightResult)(buildWrappedCrossSpec(transFromBinOp(op, MorphContext(ctx, graph))))
              } else {
                rightResult.paged(maxSliceSize).compact(valueSpec).cross(leftResult)(buildWrappedCrossSpec(flip(transFromBinOp(op, MorphContext(ctx, graph)))))
              }
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }
          
          case f @ dag.Filter(joinSort @ (CrossLeftSort | CrossRightSort), target, boolean) => 
            val isLeft = joinSort == CrossLeftSort
            
            for {
              pair <- zip(prepareEval(target, splits), prepareEval(boolean, splits))
              (pendingTableTarget, pendingTableBoolean) = pair

              targetResult = pendingTableTarget.table.transform(liftToValues(pendingTableTarget.trans))
              booleanResult = pendingTableBoolean.table.transform(liftToValues(pendingTableBoolean.trans))

              valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)

              result = if (isLeft) {
                val spec = buildWrappedCrossSpec { (srcLeft, srcRight) =>
                  trans.Filter(srcLeft, srcRight)
                }
                targetResult.paged(maxSliceSize).compact(valueSpec).cross(booleanResult)(spec)
              } else {
                val spec = buildWrappedCrossSpec { (srcLeft, srcRight) =>
                  trans.Filter(srcRight, srcLeft)
                }
                booleanResult.paged(maxSliceSize).compact(valueSpec).cross(targetResult)(spec)
              }
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }
          
          case Sort(parent, indexes) => 
            val identityOrder = Vector(0 until indexes.length: _*)
            prepareEval(parent, splits) flatMap { pending =>
              if (indexes == identityOrder && parent.sorting == IdentitySort) {
                transState liftM N.point(pending)
              } else {
                val fullOrder = indexes ++ ((0 until parent.identities.length) filterNot (indexes contains))
                val idSpec = buildIdShuffleSpec(fullOrder)
                val table = pending.table.transform(liftToValues(pending.trans))
                val shuffled = table.transform(makeTableTrans(Map(paths.Key -> idSpec)))
                
                for {
                  sorted <- transState liftM mn(shuffled.sort(DerefObjectStatic(Leaf(Source), paths.Key), SortAscending))
                  sortedResult = parent.sorting match {
                    case ValueSort(id) => sorted.transform(ObjectDelete(Leaf(Source), Set(CPathField("sort-" + id))))
                    case _ => sorted
                  }
                } yield {
                  PendingTable(sortedResult, graph, TransSpec1.Id)
                }
              }
            }
          
          case s @ SortBy(parent, sortField, valueField, id) => 
            val sortSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), CPathField(sortField))
            val valueSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), CPathField(valueField))
                  
            if (parent.sorting == ValueSort(id)) {
              prepareEval(parent, splits)
            } else {
              for {
                pending <- prepareEval(parent, splits)
                table = pending.table.transform(liftToValues(pending.trans))
                
                sorted <- transState liftM mn(table.sort(sortSpec, SortAscending))
                result = {
                  val wrappedSort = trans.WrapObject(sortSpec, "sort-" + id)
                  val wrappedValue = trans.WrapObject(valueSpec, paths.Value.name)
                  
                  val oldSortField = parent.sorting match {
                    case ValueSort(id2) if id != id2 => Some(CPathField("sort-" + id2))
                    case _ => None
                  }
                  
                  val spec = InnerObjectConcat(
                    InnerObjectConcat(
                      ObjectDelete(Leaf(Source), Set(CPathField("sort-" + id), paths.Value) ++ oldSortField),
                        wrappedSort),
                        wrappedValue)
                  
                  sorted.transform(spec)
                }
              } yield {
                PendingTable(result, graph, TransSpec1.Id)
              }
            }
         
          //ReSortBy is untested because, in practice, we cannot reach this case
          case s @ ReSortBy(parent, id) => 
            if (parent.sorting == ValueSort(id)) {
              prepareEval(parent, splits)
            } else {
              for {
                pending <- prepareEval(parent, splits)
                table = pending.table.transform(liftToValues(pending.trans))
                
                result <- transState liftM mn(table.sort(DerefObjectStatic(Leaf(Source), CPathField("sort-" + id)), SortAscending))
              } yield {
                PendingTable(result, graph, TransSpec1.Id)
              }
            }
          
          case Memoize(parent, priority) => 
            for {
              pending <- prepareEval(parent, splits)
              table = pending.table.transform(liftToValues(pending.trans))

              result <- transState liftM mn(table.force)
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }
        }
        
        val back = memoized(graph, evalTransSpecable)
        val endTime = System.nanoTime
        
        val timingM = transState liftM report.timing(graph.loc, endTime - startTime)
        
        timingM >> back
      }
    
      /**
       * The base eval function.  Takes a (rewritten) graph and evaluates the forcing
       * points at the current Split level in topological order.  The endpoint of the
       * graph is considered to be a special forcing point, but as it is the endpoint,
       * it will perforce be evaluated last.
       */
      def fullEval(graph: DepGraph, splits: Map[Identifier, Int => N[Table]], parentSplits: List[Identifier]): StateT[N, EvaluatorState, Table] = {
        import scalaz.syntax.monoid._
        
        type EvaluatorStateT[+A] = StateT[N, EvaluatorState, A]
        
        val splitNodes = splits.keys.toSet
        
        def stage(toEval: List[StagingPoint], graph: DepGraph): EvaluatorStateT[DepGraph] = {
          for {
            state <- monadState gets identity
            optPoint = toEval find { g => !(state.assume contains g) }
            
            optBack = optPoint map { point =>
              for {
                _ <- prepareEval(point, splits)
                rewritten <- stagedOptimizations(graph, ctx, optimize)
                
                toEval = listStagingPoints(Queue(rewritten)) filter referencesOnlySplit(parentSplits)
                result <- stage(toEval, rewritten)
              } yield result
            }
            
            back <- optBack getOrElse (monadState point graph)
          } yield back
        }

        // find the topologically-sorted forcing points (excluding the endpoint)
        // at the current split level
        val toEval = listStagingPoints(Queue(graph)) filter referencesOnlySplit(parentSplits)

        for {
          rewrittenGraph <- stage(toEval, graph)
          pendingTable <- prepareEval(rewrittenGraph, splits)
          table = pendingTable.table transform liftToValues(pendingTable.trans)
        } yield table
      }
    
      val resultState: StateT[N, EvaluatorState, Table] = fullEval(rewrittenDAG, Map(), Nil)

      val resultTable: N[Table] = resultState.eval(EvaluatorState())
      resultTable map { _ paged maxSliceSize compact DerefObjectStatic(Leaf(Source), paths.Value) }
    }
  
    private[this] def stagedOptimizations(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean) =
      for {
        reductions <- monadState.gets(_.reductions)
      } yield stagedRewriteDAG(optimize, ctx)(reductions.toList.foldLeft(graph) {
        case (graph, (from, Some(result))) if optimize => inlineNodeValue(graph, from, result)
        case (graph, _) => graph
      })

    /**
     * Takes a graph, a node and a value. Replaces the node (and
     * possibly its parents) with the value into the graph.
     */
    def inlineNodeValue(graph: DepGraph, from: DepGraph, result: RValue) = {
      val replacements = graph.foldDown(true) {
        case join@Join(
          DerefArray,
          CrossLeftSort,
          Join(
            DerefArray,
            CrossLeftSort,
            `from`,
            Const(CLong(index1))),
          Const(CLong(index2))) =>

          List(
            (join, Const(result)(from.loc))
          )
      }

      replacements.foldLeft(graph) {
        case (graph, (from, to)) => replaceNode(graph, from, to)
      }
    }

    private[this] def replaceNode(graph: DepGraph, from: DepGraph, to: DepGraph) = {
      graph mapDown { recurse => {
        case `from` => to
      }}
    }

    /**
     * Returns all forcing points in the graph, ordered topologically.
     */
    @tailrec
    private[this] def listStagingPoints(queue: Queue[DepGraph], acc: List[dag.StagingPoint] = Nil): List[dag.StagingPoint] = {
      def listParents(spec: BucketSpec): Set[DepGraph] = spec match {
        case UnionBucketSpec(left, right) => listParents(left) ++ listParents(right)
        case IntersectBucketSpec(left, right) => listParents(left) ++ listParents(right)
        
        case dag.Group(_, target, forest) => listParents(forest) + target
        
        case dag.UnfixedSolution(_, solution) => Set(solution)
        case dag.Extra(expr) => Set(expr)
      }
      if (queue.isEmpty) {
        acc
      } else {
        val (graph, queue2) = queue.dequeue
        
        val (queue3, addend) = {
          val queue3 = graph match {
            case _: dag.SplitParam => queue2
            case _: dag.SplitGroup => queue2
            case _: dag.Root => queue2
            
            case dag.New(parent) => queue2 enqueue parent
            
            case dag.Morph1(_, parent) => queue2 enqueue parent
            case dag.Morph2(_, left, right) => queue2 enqueue left enqueue right
            
            case dag.Distinct(parent) => queue2 enqueue parent
            
            case dag.LoadLocal(parent, _) => queue2 enqueue parent
            
            case dag.Operate(_, parent) => queue2 enqueue parent
            
            case dag.Reduce(_, parent) => queue2 enqueue parent
            case dag.MegaReduce(_, parent) => queue2 enqueue parent
            
            case dag.Split(specs, child, _) => queue2 enqueue child enqueue listParents(specs)
            
            case dag.Assert(pred, child) => queue2 enqueue pred enqueue child
            case dag.Cond(pred, left, _, right, _) => queue2 enqueue pred enqueue left enqueue right
            
            case dag.Observe(data, samples) => queue2 enqueue data enqueue samples
            
            case dag.IUI(_, left, right) => queue2 enqueue left enqueue right
            case dag.Diff(left, right) => queue2 enqueue left enqueue right
            
            case dag.Join(_, _, left, right) => queue2 enqueue left enqueue right
            case dag.Filter(_, left, right) => queue2 enqueue left enqueue right
            
            case dag.Sort(parent, _) => queue2 enqueue parent
            case dag.SortBy(parent, _, _, _) => queue2 enqueue parent
            case dag.ReSortBy(parent, _) => queue2 enqueue parent
            
            case dag.Memoize(parent, _) => queue2 enqueue parent
          }
          
          val addend = Some(graph) collect {
            case fp: StagingPoint => fp
          }
          
          (queue3, addend)
        }
        
        listStagingPoints(queue3, addend map { _ :: acc } getOrElse acc)
      }
    }
  
    // Takes a list of Splits, head is the current Split, which must be referenced.
    // The rest of the referenced Splits must be in the the list.
    private def referencesOnlySplit(parentSplits: List[Identifier])(graph: DepGraph): Boolean = {
      val referencedSplits = graph.foldDown(false) {
        case s: dag.SplitParam => Set(s.parentId)
        case s: dag.SplitGroup => Set(s.parentId)
      }

      val currentIsReferenced = parentSplits.headOption.map(referencedSplits.contains(_)).getOrElse(true)

      currentIsReferenced && (referencedSplits -- parentSplits).isEmpty
    }
  
    private def findCommonality(nodes: Set[DepGraph]): Option[DepGraph] = {
      case class Kernel(nodes: Set[DepGraph], seen: Set[DepGraph])
      
      @tailrec
      def bfs(kernels: Set[Kernel]): Set[DepGraph] = {
        // check for convergence
        val results = kernels flatMap { k =>
          val results = kernels.foldLeft(k.nodes) { _ & _.seen }
          
          // TODO if the below isEmpty, then can drop results from all kernels
          nodes.foldLeft(results) { (results, node) =>
            results filter { isTransSpecable(node, _) }
          }
        }
        
        // iterate
        if (results.isEmpty) {
          val kernels2 = kernels map { k =>
            val nodes2 = k.nodes flatMap enumerateParents
            val nodes3 = nodes2 &~ k.seen
            
            Kernel(nodes3, k.seen ++ nodes3)
          }
          
          if (kernels2 forall { _.nodes.isEmpty }) {
            Set()
          } else {
            bfs(kernels2)
          }
        } else {
          results
        }
      }
      
      if (nodes.size == 1) {
        nodes.headOption
      } else {
        val kernels = nodes map { n => Kernel(Set(n), Set(n)) }
        val results = bfs(kernels)
        
        if (results.size == 1)
          results.headOption
        else
          None
      }
    }
  
    private def enumerateParents(node: DepGraph): Set[DepGraph] = node match {
      case _: SplitParam | _: SplitGroup | _: Root => Set()
      
      case dag.New(parent) => Set(parent)
      
      case dag.Morph1(_, parent) => Set(parent)
      case dag.Morph2(_, left, right) => Set(left, right)
      
      case dag.Assert(pred, child) => Set(pred, child)
      case dag.Cond(pred, left, _, right, _) => Set(pred, left, right)
      
      case dag.Distinct(parent) => Set(parent)
      
      case dag.LoadLocal(parent, _) => Set(parent)
      
      case Operate(_, parent) => Set(parent)
      
      case dag.Reduce(_, parent) => Set(parent)
      case MegaReduce(_, parent) => Set(parent)
      
      case dag.Split(spec, _, _) => enumerateSpecParents(spec).toSet
      
      case IUI(_, left, right) => Set(left, right)
      case Diff(left, right) => Set(left, right)
      
      case Join(_, _, left, right) => Set(left, right)
      case dag.Filter(_, target, boolean) => Set(target, boolean)
      
      case Sort(parent, _) => Set(parent)
      case SortBy(parent, _, _, _) => Set(parent)
      case ReSortBy(parent, _) => Set(parent)
      
      case Memoize(parent, _) => Set(parent)
    }

    private def enumerateSpecParents(spec: BucketSpec): Set[DepGraph] = spec match {
      case UnionBucketSpec(left, right) => enumerateSpecParents(left) ++ enumerateSpecParents(right)
      case IntersectBucketSpec(left, right) => enumerateSpecParents(left) ++ enumerateSpecParents(right)
      
      case dag.Group(_, target, child) => enumerateSpecParents(child) + target
      
      case UnfixedSolution(_, target) => Set(target)
      case dag.Extra(target) => Set(target)
    }
  
    private def findCommonIds(left: BucketSpec, right: BucketSpec): Set[Int] =
      enumerateSolutionIds(left) & enumerateSolutionIds(right)
  
    private def enumerateSolutionIds(spec: BucketSpec): Set[Int] = spec match {
      case UnionBucketSpec(left, right) =>
        enumerateSolutionIds(left) ++ enumerateSolutionIds(right)
      
      case IntersectBucketSpec(left, right) =>
        enumerateSolutionIds(left) ++ enumerateSolutionIds(right)
      
      case dag.Group(_, _, forest) => enumerateSolutionIds(forest)
      
      case UnfixedSolution(id, _) => Set(id)
      case dag.Extra(_) => Set()
    }
  
    private def buildKeySpec(commonIds: Set[Int]): TransSpec1 = {
      val parts: Set[TransSpec1] = commonIds map { id =>
        trans.WrapObject(DerefObjectStatic(Leaf(Source), CPathField(id.toString)), id.toString)
      }
      
      parts reduceOption { (left, right) => trans.InnerObjectConcat(left, right) } getOrElse ConstLiteral(CEmptyArray, Leaf(Source))
    }

    private def disjunctiveEquals(specs: (IdentitySpec, IdentitySpec)): Boolean = specs match {
      case (CoproductIds(left, right), b) => disjunctiveEquals(b, left) || disjunctiveEquals(b, right)
      case (a, CoproductIds(left, right)) => disjunctiveEquals(a, left) || disjunctiveEquals(a, right)
      case (a, b) => a == b
    }
    
    private def sharedPrefixLength(left: DepGraph, right: DepGraph): Int = (left.identities, right.identities) match {
      case (Identities.Specs(a), Identities.Specs(b)) =>
        a zip b takeWhile disjunctiveEquals length
      case (Identities.Undefined, _) | (_, Identities.Undefined) =>
        0
    }

    private def enumerateGraphs(forest: BucketSpec): Set[DepGraph] = forest match {
      case UnionBucketSpec(left, right) => enumerateGraphs(left) ++ enumerateGraphs(right)
      case IntersectBucketSpec(left, right) => enumerateGraphs(left) ++ enumerateGraphs(right)
      
      case dag.Group(_, target, subForest) =>
        enumerateGraphs(subForest) + target
      
      case UnfixedSolution(_, graph) => Set(graph)
      case dag.Extra(graph) => Set(graph)
    }

    
    private def svalueToCValue(sv: SValue) = sv match {
      case STrue => CBoolean(true)
      case SFalse => CBoolean(false)
      case SString(str) => CString(str)
      case SDecimal(d) => CNum(d)
      // case SLong(l) => CLong(l)
      // case SDouble(d) => CDouble(d)
      case SNull => CNull
      case SObject(obj) if obj.isEmpty => CEmptyObject
      case SArray(Vector()) => CEmptyArray
      case _ => sys.error("die a horrible death: " + sv)
    }
    
    private def join(left: Table, right: Table)(key: TransSpec1, spec: TransSpec2): Table = {
      val emptySpec = trans.ConstLiteral(CEmptyArray, Leaf(Source))
      val result = left.cogroup(key, key, right)(emptySpec, emptySpec, trans.WrapArray(spec))

      result.transform(trans.DerefArrayStatic(Leaf(Source), CPathIndex(0)))
    }
    
    private def flip[A, B, C](f: (A, B) => C)(b: B, a: A): C = f(a, b)      // is this in scalaz?
    
    private def zip[A](table1: StateT[N, EvaluatorState, A], table2: StateT[N, EvaluatorState, A]): StateT[N, EvaluatorState, (A, A)] =
      monadState.apply2(table1, table2) { (_, _) }

    private case class EvaluatorState(
      assume: Map[DepGraph, Table] = Map.empty,
      reductions: Map[DepGraph, Option[RValue]] = Map.empty,
      extraCount: Int = 0
    )

    private case class PendingTable(table: Table, graph: DepGraph, trans: TransSpec1)
  }
}
