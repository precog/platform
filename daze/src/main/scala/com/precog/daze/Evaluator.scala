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

import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.util.IdSourceConfig
import com.precog.util._
import com.precog.common.json._
import com.precog.common.{Path, VectorCase}
import com.precog.common.json.{CPath, CPathField, CPathIndex}
import com.precog.bytecode._

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._
import collection.immutable.ListSet

import akka.dispatch.Future

import scalaz.{NonEmptyList => NEL, _}
import scalaz.Id._
import scalaz.State._
import scalaz.StateT._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.partialFunction._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import scala.collection.immutable.Queue

import com.weiglewilczek.slf4s.Logging

trait EvaluatorConfig extends IdSourceConfig {
  def maxEvalDuration: akka.util.Duration
  def maxSliceSize: Int
}

trait Evaluator[M[+_]] extends DAG
    with CrossOrdering
    with Memoizer
    with TypeInferencer
    with JoinOptimizer
    with StaticInliner[M]
    with ReductionFinder
    with TableModule[M]        // TODO specific implementation
    with ImplLibrary[M]
    with InfixLib[M]
    with UnaryLib[M]
    with BigDecimalOperations
    with YggConfigComponent 
    with Logging { self =>

  type UserId = String
  
  type MemoId = Int
  type GroupId = Int
  
  import Function._
  
  import instructions._
  import dag._
  import trans._
  import constants._
  import TableModule._

  type YggConfig <: EvaluatorConfig
  
  def withContext[A](f: Context => A): A = {
    f(new Context { })
  }

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def PrimitiveEqualsF2: F2
  def ConstantEmptyArray: F1
  
  def freshIdScanner: Scanner

  def rewriteDAG(optimize: Boolean): DepGraph => DepGraph = {
    (if (optimize) inlineStatics(_) else identity[DepGraph] _) andThen
    (if (optimize) optimizeJoins(_) else identity) andThen
    (orderCrosses _) andThen
    (if (optimize) (g => megaReduce(g, findReductions(g))) else identity) andThen
    (if (optimize) inferTypes(JType.JUnfixedT) else identity) andThen
    (if (optimize) (memoize _) else identity)
  }
  
  /**
   * The entry point to the evaluator.  The main implementation of the evaluator
   * is comprised by the inner functions, `fullEval` (the main evaluator function)
   * and `prepareEval` (which has the primary eval loop).
   */
  def eval(userUID: UserId, graph: DepGraph, ctx: Context, prefix: Path, optimize: Boolean): M[Table] = {
    logger.debug("Eval for %s = %s".format(userUID.toString, graph))
  
    def resolveTopLevelGroup(spec: BucketSpec, splits: Map[dag.Split, (Table, Int => M[Table])]): StateT[Id, EvaluatorState, M[GroupingSpec]] = spec match {
      case UnionBucketSpec(left, right) => {
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
      }
      
      case IntersectBucketSpec(left, right) => {
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
      }
      
      case dag.Group(id, target, forest) => {
        val common = findCommonality(enumerateGraphs(forest) + target)
        
        common match {
          case Some(reducedTarget) => {
            for {
              pendingTargetTable <- prepareEval(reducedTarget, splits)
            //val PendingTable(reducedTargetTableF, _, reducedTargetTrans) = prepareEval(reducedTarget, assume, splits)
           
              resultTargetTable = pendingTargetTable.table map { _.transform { liftToValues(pendingTargetTable.trans) } }
              
              // TODO FIXME if the target has forcing points, targetTrans is insufficient
              //val PendingTable(_, _, targetTrans) = prepareEval(target, assume + (reducedTarget -> resultTargetTable), splits)
                
              state <- get[EvaluatorState]
              trans = prepareEval(target, splits).eval(state.copy(assume = state.assume + (reducedTarget -> resultTargetTable))).trans
              subSpec <- resolveLowLevelGroup(resultTargetTable, reducedTarget, forest, splits)
            } yield {
              resultTargetTable map { GroupingSource(_, SourceKey.Single, Some(liftToValues(trans)), id, subSpec) }
            }
          }
          
          case None => sys.error("O NOES!!!")
        }
      }
      
      case UnfixedSolution(_, _) | dag.Extra(_) => sys.error("assertion error")
    }

    //** only used in resolveTopLevelGroup **/
    def resolveLowLevelGroup(commonTable: M[Table], commonGraph: DepGraph, forest: BucketSpec, splits: Map[dag.Split, (Table, Int => M[Table])]): StateT[Id, EvaluatorState, GroupKeySpec] = forest match {
      case UnionBucketSpec(left, right) => {
        for {
          leftRes <- resolveLowLevelGroup(commonTable, commonGraph, left, splits)
          rightRes <- resolveLowLevelGroup(commonTable, commonGraph, right, splits)
        } yield GroupKeySpecOr(leftRes, rightRes)
      }
      
      case IntersectBucketSpec(left, right) => {
        for {
          leftRes <- resolveLowLevelGroup(commonTable, commonGraph, left, splits)
          rightRes <- resolveLowLevelGroup(commonTable, commonGraph, right, splits)
        } yield GroupKeySpecAnd(leftRes, rightRes)
      }
      
      case UnfixedSolution(id, solution) => {
        for {
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (commonGraph -> commonTable)) }
          pendingTable <- prepareEval(solution, splits)
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume - commonGraph) }

          liftedTrans = TransSpec.deepMap(pendingTable.trans) {
            case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
          }
        } yield GroupKeySpecSource(CPathField(id.toString), liftedTrans)
      }
      
      case dag.Extra(graph) => {
        for {
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (commonGraph -> commonTable)) }
          pendingTable <- prepareEval(graph, splits)
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume - commonGraph) }
          
          state <- get[EvaluatorState]
          extraId = state.extraCount
          _ <- modify[EvaluatorState] { _.copy(extraCount = extraId + 1) }

          liftedTrans = TransSpec.deepMap(pendingTable.trans) {
            case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
          }
        } yield GroupKeySpecSource(CPathField("extra" + extraId), trans.Filter(liftedTrans, liftedTrans))
      }
      
      case dag.Group(_, _, _) => sys.error("assertion error")
    }

    def prepareEval(graph: DepGraph, splits: Map[dag.Split, (Table, Int => M[Table])]): StateT[Id, EvaluatorState, PendingTable] = {
      logger.trace("Loop on %s".format(graph))
      
      val assumptionCheck: StateT[Id, EvaluatorState, Option[M[Table]]] = for {
        state <- get[EvaluatorState]
      } yield state.assume.get(graph)
      
      def result: StateT[Id, EvaluatorState, PendingTable] = graph match {
        case s @ SplitParam(_, index) => {
          val (key, _) = splits(s.parent)
          
          val source = trans.DerefObjectStatic(Leaf(Source), CPathField(index.toString))
          val spec = buildConstantWrapSpec(source)
          
          state(PendingTable(M.point(key transform spec), graph, TransSpec1.Id))
        }
        
        case s @ SplitGroup(_, index, _) => {
          val (_, f) = splits(s.parent)
          state(PendingTable(f(index), graph, TransSpec1.Id))
        }
        
        case Root(_, value) => {
          val table = value match {
            case str @ CString(_) => Table.constString(Set(str))
            
            case ln @ CLong(_) => Table.constLong(Set(ln))
            case d @ CDouble(_) => Table.constDouble(Set(d))
            case n @ CNum(_) => Table.constDecimal(Set(n))
            
            case b @ CBoolean(_) => Table.constBoolean(Set(b))
            
            case CNull => Table.constNull
            
            case CEmptyObject => Table.constEmptyObject
            case CEmptyArray => Table.constEmptyArray
            
            case CUndefined => Table.empty
          }
          
          val spec = buildConstantWrapSpec(Leaf(Source))
          
          state(PendingTable(M.point(table.transform(spec)), graph, TransSpec1.Id))
        }
        
        // TODO technically, we can do this without forcing by pre-lifting PendingTable#trans
        case dag.New(_, parent) => {
          for {
            pendingTable <- prepareEval(parent, splits)
            idSpec = TableTransSpec.makeTransSpec(
              Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))
            
            tableM2 = for {
              table <- pendingTable.table
              remapped = table.transform(liftToValues(pendingTable.trans)).transform(idSpec)
            } yield remapped
          } yield PendingTable(tableM2, graph, TransSpec1.Id)
        }
        
        case dag.LoadLocal(_, parent, jtpe) => {
          for {
            pendingTable <- prepareEval(parent, splits)
            Path(prefixStr) = prefix
            f1 = Infix.concatString.f2.partialLeft(CString(prefixStr.replaceAll("/$", "")))
            trans2 = trans.Map1(trans.DerefObjectStatic(pendingTable.trans, paths.Value), f1)
            val back = pendingTable.table flatMap { _.transform(trans2).load(userUID, jtpe) }
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Morph1(_, mor, parent) => {
          for {
            pendingTable <- prepareEval(parent, splits)
            val back = pendingTable.table flatMap { table => mor(table.transform(liftToValues(pendingTable.trans))) }
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Morph2(_, mor, left, right) => {
          lazy val spec = trans.ArrayConcat(trans.WrapArray(Leaf(SourceLeft)), trans.WrapArray(Leaf(SourceRight)))
          lazy val specRight = trans.ArrayConcat(trans.WrapArray(Leaf(SourceRight)), trans.WrapArray(Leaf(SourceLeft)))
          lazy val key = trans.DerefObjectStatic(Leaf(Source), paths.Key)
          
          for {
            pendingTableLeft <- prepareEval(left, splits)
            pendingTableRight <- prepareEval(right, splits)
            
            val back = for {
              leftTable <- pendingTableLeft.table
              val leftResult = leftTable.transform(liftToValues(pendingTableLeft.trans))
              
              rightTable <- pendingTableRight.table
              val rightResult = rightTable.transform(liftToValues(pendingTableRight.trans))
            
              val aligned = mor.alignment match {
                case MorphismAlignment.Cross => leftResult.cross(rightResult)(spec)
                case MorphismAlignment.Match if sharedPrefixLength(left, right) > 0 => join(leftResult, rightResult)(key, spec)
                case MorphismAlignment.Match if sharedPrefixLength(left, right) == 0 => {
                  if (left.isSingleton) {
                    rightResult.cross(leftResult)(specRight) 
                  } else if (right.isSingleton) {
                    leftResult.cross(rightResult)(spec) 
                  } else {
                    rightResult.cross(leftResult)(specRight) 
                  }
                }
              }
              leftSpec = DerefObjectStatic(DerefArrayStatic(TransSpec1.Id, CPathIndex(0)), paths.Value)
              rightSpec = DerefObjectStatic(DerefArrayStatic(TransSpec1.Id, CPathIndex(1)), paths.Value)
              transformed = aligned.transform(ArrayConcat(trans.WrapArray(leftSpec), trans.WrapArray(rightSpec)))

              result <- mor(transformed)
            } yield result

          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Distinct(_, parent) => {
          val idSpec = TableTransSpec.makeTransSpec(
            Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))

          for {
            pending <- prepareEval(parent, splits)
          } yield {
            val keySpec   = DerefObjectStatic(Leaf(Source), paths.Key)
            val valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
            
            val result = for {
              pendingTable <- pending.table
              table = pendingTable.transform(liftToValues(pending.trans))
              sorted <- table.sort(valueSpec, SortAscending)
              distinct = sorted.distinct(valueSpec) 
            } yield distinct.transform(idSpec)
            PendingTable(result, graph, TransSpec1.Id)
          }
        }

        case Operate(_, instructions.WrapArray, parent) => {
          for {
            pendingTable <- prepareEval(parent, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.WrapArray(pendingTable.trans))
        }
        
        case o @ Operate(_, op, parent) => {
          for {
            pendingTable <- prepareEval(parent, splits)
            
            // TODO unary typing
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, op1(op).f1))
        }

        /**
        returns an array (to be dereferenced later) containing the result of each reduction
        */
        case m @ MegaReduce(_, reds, parent) => {
          val red: ReductionImpl = coalesce(reds)  

          for {
            pendingTable <- prepareEval(parent, splits)
            liftedTrans = liftToValues(pendingTable.trans)

            result = pendingTable.table flatMap { parentTable => red(parentTable.transform(DerefObjectStatic(liftedTrans, paths.Value))) }
            keyWrapped = trans.WrapObject(trans.ConstLiteral(CEmptyArray, trans.DerefArrayStatic(Leaf(Source), CPathIndex(0))), paths.Key.name)  //TODO deref by index 0 is WRONG
            valueWrapped = trans.InnerObjectConcat(keyWrapped, trans.WrapObject(Leaf(Source), paths.Value.name))
            wrapped = result map { _ transform valueWrapped }
            _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (m -> wrapped)) }
          } yield {
            PendingTable(wrapped, graph, TransSpec1.Id)
          }
        }
        
        case r @ dag.Reduce(_, red, parent) => {
          for {
            pendingTable <- prepareEval(parent, splits)
            liftedTrans = liftToValues(pendingTable.trans)
            result = pendingTable.table flatMap { parentTable => red(parentTable.transform(DerefObjectStatic(liftedTrans, paths.Value))) }
            wrapped = result map { _ transform buildConstantWrapSpec(Leaf(Source)) }
          } yield PendingTable(wrapped, graph, TransSpec1.Id)
        }
        
        case s @ dag.Split(line, spec, child) => {
          val idSpec = TableTransSpec.makeTransSpec(
            Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))

          val table = for {
            grouping <- resolveTopLevelGroup(spec, splits)
            state <- get[EvaluatorState]
          } yield {
            for {
              grouping2 <- grouping
              result <- Table.merge(grouping2) { (key: Table, map: Int => M[Table]) =>
                val back = fullEval(child, splits + (s -> (key -> map)), Some(s))

                back.eval(state)  //: M[Table]
              }
            } yield result.transform(idSpec)
          }
          table map { PendingTable(_, graph, TransSpec1.Id) }
        }
        
        case IUI(_, union, left, right) => {
          for {
            leftPending <- prepareEval(left, splits)
            rightPending <- prepareEval(right, splits)
          } yield {
            val keyValueSpec = TransSpec1.PruneToKeyValue              

            val result = for {
              leftPendingTable <- leftPending.table
              rightPendingTable <- rightPending.table

              leftTable = leftPendingTable.transform(liftToValues(leftPending.trans))
              rightTable = rightPendingTable.transform(liftToValues(rightPending.trans))

              leftSorted <- leftTable.sort(keyValueSpec, SortAscending)
              rightSorted <- rightTable.sort(keyValueSpec, SortAscending)
            } yield {
              if (union) {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(Leaf(Source), Leaf(Source), Leaf(SourceLeft))
              } else {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(TransSpec1.DeleteKeyValue, TransSpec1.DeleteKeyValue, TransSpec2.LeftId)
              }
            }

            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        // TODO unify with IUI
        case Diff(_, left, right) =>{
          for {
            leftPending <- prepareEval(left, splits)
            rightPending <- prepareEval(right, splits)
          } yield {
            val result = for {
              leftPendingTable <- leftPending.table
              rightPendingTable <- rightPending.table

              leftTable = leftPendingTable.transform(liftToValues(leftPending.trans))
              rightTable = rightPendingTable.transform(liftToValues(rightPending.trans))
              
              // this transspec prunes everything that is not a key or a value.
              keyValueSpec = TransSpec1.PruneToKeyValue

              leftSorted <- leftTable.sort(keyValueSpec, SortAscending)
              rightSorted <- rightTable.sort(keyValueSpec, SortAscending)
            } yield {
              leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(TransSpec1.Id, TransSpec1.DeleteKeyValue, TransSpec2.DeleteKeyValueLeft)
            }
            
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        case Join(_, Eq, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          for {
            pendingTable <- prepareEval(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, value, false))
        }
        
        case Join(_, Eq, CrossLeftSort | CrossRightSort, Root(_, value), right) => {
          for {
            pendingTable <- prepareEval(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, value, false))
        }
        
        case Join(_, NotEq, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          for {
            pendingTable <- prepareEval(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, value, true))
        }
        
        case Join(_, NotEq, CrossLeftSort | CrossRightSort, Root(_, value), right) => {
          for {
            pendingTable <- prepareEval(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, value, true))
        }
        
        case Join(_, instructions.WrapObject, CrossLeftSort | CrossRightSort, Root(_, value), right) => {
          value match {
            case value @ CString(str) => {
              //prepareEval(right, splits) map { pendingTable => PendingTable(pendingTable.table, pendingTable.graph, trans.WrapObject(pendingTable.trans, str)) }

              for {
                pendingTable <- prepareEval(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.WrapObject(pendingTable.trans, str))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, DerefObject, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          value match {
            case value @ CString(str) => {
              for {
                pendingTable <- prepareEval(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefObjectStatic(pendingTable.trans, CPathField(str)))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, DerefMetadata, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          value match {
            case value @ CString(str) => {
              for {
                pendingTable <- prepareEval(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefMetadataStatic(pendingTable.trans, CPathMeta(str)))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, DerefArray, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          val optIndex = value match {
            case CNum(d) => Some(d.toInt)
            case CLong(ln) => Some(ln.toInt)
            case CDouble(d) => Some(d.toInt)
            case _ => None
          }
          
          optIndex map { idx => 
            for {
              pendingTable <- prepareEval(left, splits)
            } yield PendingTable(pendingTable.table, pendingTable.graph, DerefArrayStatic(pendingTable.trans, CPathIndex(idx)))
          } getOrElse state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
        }
        
        case Join(_, instructions.ArraySwap, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          val optIndex = value match {
            case CNum(d) => Some(d.toInt)
            case CLong(ln) => Some(ln.toInt)
            case CDouble(d) => Some(d.toInt)
            case _ => None
          }
          
          optIndex map { idx =>
            for {
              pendingTable <- prepareEval(left, splits)
            } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArraySwap(pendingTable.trans, idx))
          } getOrElse state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
        }
        
        case Join(_, instructions.JoinObject, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          value match {
            case CEmptyObject => {
              for {
                pendingTable <- prepareEval(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.InnerObjectConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinObject, CrossLeftSort | CrossRightSort, Root(_, value), right) => {
          value match {
            case CEmptyObject => {
              for {
                pendingTable <- prepareEval(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.InnerObjectConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinArray, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          value match {
            case CEmptyArray => {
              for {
                pendingTable <- prepareEval(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArrayConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinArray, CrossLeftSort | CrossRightSort, Root(_, value), right) => {
          value match {
            case CEmptyArray => {
              for {
                pendingTable <- prepareEval(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArrayConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
  
        // begin: annoyance with Scala's lousy pattern matcher
        case Join(_, op, CrossLeftSort | CrossRightSort, left, Root(_, value)) => {
          op2ForBinOp(op) map { _.f2.partialRight(value) } map { f1 =>
            for {
              pendingTable <- prepareEval(left, splits)
            } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, f1))
          } getOrElse state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
        }
        
        case Join(_, op, CrossLeftSort | CrossRightSort, Root(_, value), right) => {
          op2ForBinOp(op) map { _.f2.partialLeft(value) } map { f1 =>
            for {
              pendingTable <- prepareEval(right, splits)
            } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, f1))
          } getOrElse state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
        }
        // end: annoyance
        
        case Join(_, op, joinSort @ (IdentitySort | ValueSort(_)), left, right) => {
          // TODO binary typing

          for {
            pendingTableLeft <- prepareEval(left, splits)
            pendingTableRight <- prepareEval(right, splits)
          } yield {
            if (pendingTableLeft.graph == pendingTableRight.graph) {
              PendingTable(pendingTableLeft.table, pendingTableLeft.graph, transFromBinOp(op)(pendingTableLeft.trans, pendingTableRight.trans))
            } else {
              val prefixLength = sharedPrefixLength(left, right)
              
              val key = joinSort match {
                case IdentitySort =>
                  buildJoinKeySpec(prefixLength)
                
                case ValueSort(id) =>
                  trans.DerefObjectStatic(Leaf(Source), CPathField("sort-" + id))
                
                case _ => sys.error("unreachable code")
              }
              
              val spec = buildWrappedJoinSpec(prefixLength, left.identities.length, right.identities.length)(transFromBinOp(op))

              val result = for {
                parentLeftTable <- pendingTableLeft.table
                val leftResult = parentLeftTable/*.printer("left before transform: ")*/.transform(liftToValues(pendingTableLeft.trans))//.printer("left transformed: ")
                
                parentRightTable <- pendingTableRight.table
                val rightResult = parentRightTable/*.printer("right before transform: ")*/.transform(liftToValues(pendingTableRight.trans))//.printer("right transformed: ")

              } yield join(leftResult, rightResult)(key, spec)

              PendingTable(result, graph, TransSpec1.Id)
            } 
          }
        }
  
        case j @ Join(_, op, joinSort @ (CrossLeftSort | CrossRightSort), left, right) => {
          val isLeft = joinSort == CrossLeftSort

          for {
            pendingTableLeft <- prepareEval(left, splits)
            pendingTableRight <- prepareEval(right, splits)
          } yield {
            val result = for {
              parentLeftTable <- pendingTableLeft.table 
              val leftResult = parentLeftTable.transform(liftToValues(pendingTableLeft.trans))
              
              parentRightTable <- pendingTableRight.table 
              val rightResult = parentRightTable.transform(liftToValues(pendingTableRight.trans))
            } yield {
              val valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
              
              if (isLeft)
                leftResult.paged(maxSliceSize).compact(valueSpec).cross(rightResult)(buildWrappedCrossSpec(transFromBinOp(op)))
              else
                rightResult.paged(maxSliceSize).compact(valueSpec).cross(leftResult)(buildWrappedCrossSpec(flip(transFromBinOp(op))))
            }
            
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        case dag.Filter(_, joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => {
          // TODO binary typing

          for {
            pendingTableTarget <- prepareEval(target, splits)
            pendingTableBoolean <- prepareEval(boolean, splits)
          } yield {
            if (pendingTableTarget.graph == pendingTableBoolean.graph) {
              PendingTable(pendingTableTarget.table, pendingTableTarget.graph, trans.Filter(pendingTableTarget.trans, pendingTableBoolean.trans))
            } else {
              val key = joinSort match {
                case IdentitySort =>
                  trans.DerefObjectStatic(Leaf(Source), paths.Key)
                
                case ValueSort(id) =>
                  trans.DerefObjectStatic(Leaf(Source), CPathField("sort-" + id))
                
                case _ => sys.error("unreachable code")
              }

              val spec = buildWrappedJoinSpec(sharedPrefixLength(target, boolean), target.identities.length, boolean.identities.length) { (srcLeft, srcRight) =>
                trans.Filter(srcLeft, srcRight)
              }

              val result = for {
                parentTargetTable <- pendingTableTarget.table 
                val targetResult = parentTargetTable.transform(liftToValues(pendingTableTarget.trans))
                
                parentBooleanTable <- pendingTableBoolean.table
                val booleanResult = parentBooleanTable.transform(liftToValues(pendingTableBoolean.trans))
              } yield join(targetResult, booleanResult)(key, spec)

              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case f @ dag.Filter(_, joinSort @ (CrossLeftSort | CrossRightSort), target, boolean) => {
          val isLeft = joinSort == CrossLeftSort
          
          /* target match {
            case Join(_, Map2Cross(Eq) | Map2CrossLeft(Eq) | Map2CrossRight(Eq), left, right) => {
              
            }
          } */
          
          for {
            pendingTableTarget <- prepareEval(target, splits)
            pendingTableBoolean <- prepareEval(boolean, splits)
          } yield {
            val result = for {
              parentTargetTable <- pendingTableTarget.table 
              val targetResult = parentTargetTable.transform(liftToValues(pendingTableTarget.trans))
              
              parentBooleanTable <- pendingTableBoolean.table
              val booleanResult = parentBooleanTable.transform(liftToValues(pendingTableBoolean.trans))
            } yield {
              val valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
              
              if (isLeft) {
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
            }
            
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        case s @ Sort(parent, indexes) => {
          if (indexes == Vector(0 until indexes.length: _*) && parent.sorting == IdentitySort) {
            prepareEval(parent, splits)
          } else {
            val fullOrder = indexes ++ ((0 until parent.identities.length) filterNot (indexes contains))
            val idSpec = buildIdShuffleSpec(fullOrder)
            
            for {
              pending <- prepareEval(parent, splits)
              
              sortedResult = for {
                pendingTable <- pending.table
                val table = pendingTable.transform(liftToValues(pending.trans))
                val shuffled = table.transform(TableTransSpec.makeTransSpec(Map(paths.Key -> idSpec)))
                // TODO this could be made more efficient by only considering the indexes we care about
                sorted <- shuffled.sort(DerefObjectStatic(Leaf(Source), paths.Key), SortAscending)
              } yield {                              
                parent.sorting match {
                  case ValueSort(id) =>
                    sorted.transform(ObjectDelete(Leaf(Source), Set(CPathField("sort-" + id))))
                    
                  case _ => sorted
                }
              }
            } yield {
              PendingTable(sortedResult, graph, TransSpec1.Id)
            }
          }
        }
        
        case s @ SortBy(parent, sortField, valueField, id) => {
          val sortSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), CPathField(sortField))
          val valueSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), CPathField(valueField))
                
          if (parent.sorting == ValueSort(id)) {
            prepareEval(parent, splits)
          } else {
            for {
              pending <- prepareEval(parent, splits)
              
              result = for {
                pendingTable <- pending.table
                table = pendingTable.transform(liftToValues(pending.trans))
                sorted <- table.sort(sortSpec, SortAscending)
              } yield {
                val wrappedSort = trans.WrapObject(sortSpec, "sort-" + id)
                val wrappedValue = trans.WrapObject(valueSpec, paths.Value.name)
                
                val oldSortField = parent.sorting match {
                  case ValueSort(id2) if id != id2 =>
                    Some(CPathField("sort-" + id2))
                  
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
        }
       
        //ReSortBy is untested because, in practice, we cannot reach this case
        case s @ ReSortBy(parent, id) => {
          if (parent.sorting == ValueSort(id)) {
            prepareEval(parent, splits)
          } else {
            for {
              pending <- prepareEval(parent, splits)
              
              result = for {
                pendingTable <- pending.table
                val table = pendingTable.transform(liftToValues(pending.trans))
                sorted <- table.sort(DerefObjectStatic(Leaf(Source), CPathField("sort-" + id)), SortAscending)
              } yield sorted
            } yield {
              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case Memoize(parent, priority) => {
          for {
            pending <- prepareEval(parent, splits)
            
            result = for {
              pendingTable <- pending.table
              table = pendingTable.transform(liftToValues(pending.trans))
              forced <- table.force
            } yield forced
          } yield {
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
      }
      
      def memoizedResult = graph match {
        case graph: StagingPoint => {
          for {
            pendingTable <- result
            _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (graph -> pendingTable.table)) }
          } yield pendingTable
        }
        
        case _ => result
      }

      assumptionCheck flatMap { assumedResult: Option[M[Table]] =>
        val liftedAssumption = assumedResult map { table =>
          state[EvaluatorState, PendingTable](PendingTable(table, graph, TransSpec1.Id))
        }
        
        liftedAssumption getOrElse memoizedResult
      }
    }
    
    /**
     * The base eval function.  Takes a (rewritten) graph and evaluates the forcing
     * points at the current Split level in topological order.  The endpoint of the
     * graph is considered to be a special forcing point, but as it is the endpoint,
     * it will perforce be evaluated last.
     */
    def fullEval(graph: DepGraph, splits: Map[dag.Split, (Table, Int => M[Table])], currentSplit: Option[dag.Split]): StateT[Id, EvaluatorState, M[Table]] = {
      import scalaz.syntax.monad._
      import scalaz.syntax.monoid._
      
      // find the topologically-sorted forcing points (excluding the endpoint)
      // at the current split level
      val toEval = listStagingPoints(Queue(graph)) filter referencesOnlySplit(currentSplit)
      
      val preStates = toEval map { graph =>
        for {
          _ <- prepareEval(graph, splits)
        } yield ()        // the result is uninteresting, since it has been stored in `assumed`
      }
      
      val preState = preStates reduceOption { _ >> _ } getOrElse StateT.stateT[Id, EvaluatorState, Unit](())
      
      // run the evaluator on all forcing points *including* the endpoint, in order
      for {
        pendingTable <- preState >> prepareEval(graph, splits)
        table = pendingTable.table map { _ transform liftToValues(pendingTable.trans) }
      } yield table
    }
    
    val resultState: StateT[Id, EvaluatorState, M[Table]] = 
      fullEval(rewriteDAG(optimize)(graph), Map(), None)

    (resultState.eval(EvaluatorState(Map())): M[Table]) map { _ paged maxSliceSize compact DerefObjectStatic(Leaf(Source), paths.Value) }
  }
  
  /**
   * Returns all forcing points in the graph, ordered topologically.
   */
  private def listStagingPoints(queue: Queue[DepGraph], acc: List[dag.StagingPoint] = Nil): List[dag.StagingPoint] = {
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
          
          case dag.New(_, parent) => queue2 enqueue parent
          
          case dag.Morph1(_, _, parent) => queue2 enqueue parent
          case dag.Morph2(_, _, left, right) => queue2 enqueue left enqueue right
          
          case dag.Distinct(_, parent) => queue2 enqueue parent
          
          case dag.LoadLocal(_, parent, _) => queue2 enqueue parent
          
          case dag.Operate(_, _, parent) => queue2 enqueue parent
          
          case dag.Reduce(_, _, parent) => queue2 enqueue parent
          case dag.MegaReduce(_, _, parent) => queue2 enqueue parent
          
          case dag.Split(_, specs, child) => queue2 enqueue child enqueue listParents(specs)
          
          case dag.IUI(_, _, left, right) => queue2 enqueue left enqueue right
          case dag.Diff(_, left, right) => queue2 enqueue left enqueue right
          
          case dag.Join(_, _, _, left, right) => queue2 enqueue left enqueue right
          case dag.Filter(_, _, left, right) => queue2 enqueue left enqueue right
          
          case dag.Sort(parent, _) => queue2 enqueue parent
          case dag.SortBy(parent, _, _, _) => queue2 enqueue parent
          case dag.ReSortBy(parent, _) => queue2 enqueue parent
          
          case node @ dag.Memoize(parent, _) => queue2 enqueue parent
        }
        
        val addend = Some(graph) collect {
          case fp: StagingPoint => fp
        }
        
        (queue3, addend)
      }
      
      listStagingPoints(queue3, addend map { _ :: acc } getOrElse acc)
    }
  }
  
  private def referencesOnlySplit(split: Option[dag.Split])(graph: DepGraph): Boolean = {
    implicit val m: Monoid[Boolean] = new Monoid[Boolean] {
      def zero = true
      def append(b1: Boolean, b2: => Boolean): Boolean = b1 && b2
    }
    
    graph foldDown {
      case s: dag.SplitParam => split map (s.parent ==) getOrElse false
      case s: dag.SplitGroup => split map (s.parent ==) getOrElse false
    }
  }
  
  private def findCommonality(forest: Set[DepGraph]): Option[DepGraph] = {
    if (forest.size == 1) {
      Some(forest.head)
    } else {
      val sharedPrefixReversed = forest flatMap buildChains map { _.reverse } reduceOption { (left, right) =>
        left zip right takeWhile { case (a, b) => a == b } map { _._1 }
      }

      sharedPrefixReversed flatMap { _.lastOption }
    }
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
  
  private def buildChains(graph: DepGraph): Set[List[DepGraph]] = {
    val parents = enumerateParents(graph)
    val recursive = parents flatMap buildChains map { graph :: _ }
    if (!parents.isEmpty && recursive.isEmpty) Set(graph :: Nil) else recursive
  }
  
  private def enumerateParents(graph: DepGraph): Set[DepGraph] = graph match {
    case SplitParam(_, _) => Set()
    case SplitGroup(_, _, _) => Set()
    case Root(_, _) => Set()
    case dag.New(_, parent) => Set(parent)
    case dag.Morph1(_, _, parent) => Set(parent)
    case dag.Morph2(_, _, left, right) => Set(left, right)
    case dag.Distinct(_, parent) => Set(parent)
    case dag.LoadLocal(_, parent, _) => Set(parent)
    case Operate(_, _, parent) => Set(parent)
    case dag.Reduce(_, _, parent) => Set(parent)
    case dag.MegaReduce(_, _, parent) => Set(parent)
    case dag.Split(_, spec, _) => enumerateGraphs(spec)
    case Join(_, _, _, left, right) => Set(left, right)
    case dag.Filter(_, _, target, boolean) => Set(target, boolean)
    case dag.Sort(parent, _) => Set(parent)
    case dag.SortBy(parent, _, _, _) => Set(parent)
    case Memoize(parent, _) => Set(parent)
  }
  
  private def enumerateGraphs(forest: BucketSpec): Set[DepGraph] = forest match {
    case UnionBucketSpec(left, right) => enumerateGraphs(left) ++ enumerateGraphs(right)
    case IntersectBucketSpec(left, right) => enumerateGraphs(left) ++ enumerateGraphs(right)
    
    case dag.Group(_, target, subForest) =>
      enumerateGraphs(subForest) + target
    
    case UnfixedSolution(_, graph) => Set(graph)
    case dag.Extra(graph) => Set(graph)
  }
  
  private def op1(op: UnaryOperation): Op1 = op match {
    case BuiltInFunction1Op(op1) => op1
    
    case instructions.New | instructions.WrapArray => sys.error("assertion error")
    
    case Comp => Unary.Comp
    case Neg => Unary.Neg
  }
  
  private def transFromBinOp[A <: SourceType](op: BinaryOperation)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
    case Eq => trans.Equal(left, right)
    case NotEq => trans.Map1(trans.Equal(left, right), op1(Comp).f1)
    case instructions.WrapObject => WrapObjectDynamic(left, right)
    case JoinObject => InnerObjectConcat(left, right)
    case JoinArray => ArrayConcat(left, right)
    case instructions.ArraySwap => sys.error("nothing happens")
    case DerefObject => DerefObjectDynamic(left, right)
    case DerefMetadata => sys.error("cannot do a dynamic metadata deref")
    case DerefArray => DerefArrayDynamic(left, right)
    case _ => trans.Map2(left, right, op2ForBinOp(op).get.f2)     // if this fails, we're missing a case above
  }

  private def sharedPrefixLength(left: DepGraph, right: DepGraph): Int =
    left.identities zip right.identities takeWhile { case (a, b) => a == b } length
  
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
  
  private def buildConstantWrapSpec[A <: SourceType](source: TransSpec[A]): TransSpec[A] = {  //TODO don't use Map1, returns an empty array of type CNum
    val bottomWrapped = trans.WrapObject(trans.ConstLiteral(CEmptyArray, source), paths.Key.name)
    trans.InnerObjectConcat(bottomWrapped, trans.WrapObject(source, paths.Value.name))
  }

  private def buildValueWrapSpec[A <: SourceType](source: TransSpec[A]): TransSpec[A] = {
    trans.WrapObject(source, paths.Value.name)
  }
  
  private def buildJoinKeySpec(sharedLength: Int): TransSpec1 = {
    val components = for (i <- 0 until sharedLength)
      yield trans.WrapArray(DerefArrayStatic(SourceKey.Single, CPathIndex(i))): TransSpec1
    
    components reduce { trans.ArrayConcat(_, _) }
  }
  
  private def buildWrappedJoinSpec(sharedLength: Int, leftLength: Int, rightLength: Int)(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
    val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
    val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)
    
    val sharedDerefs = for (i <- 0 until sharedLength)
      yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, CPathIndex(i)))
    
    val unsharedLeft = for (i <- sharedLength until leftLength)
      yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, CPathIndex(i)))
    
    val unsharedRight = for (i <- sharedLength until rightLength)
      yield trans.WrapArray(DerefArrayStatic(rightIdentitySpec, CPathIndex(i)))
    
    val derefs: Seq[TransSpec2] = sharedDerefs ++ unsharedLeft ++ unsharedRight
    
    val newIdentitySpec = if (derefs.isEmpty)
      trans.ConstLiteral(CEmptyArray, Leaf(SourceLeft))
    else
      derefs reduce { trans.ArrayConcat(_, _) }
    
    val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, paths.Key.name)
    
    val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), paths.Value)
    val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), paths.Value)
    
    val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), paths.Value.name)
      
    InnerObjectConcat(wrappedValueSpec, wrappedIdentitySpec)
  }
  
  private def buildWrappedCrossSpec(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
    val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
    val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)
    
    val newIdentitySpec = ArrayConcat(leftIdentitySpec, rightIdentitySpec)
    
    val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, paths.Key.name)

    val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), paths.Value)
    val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), paths.Value)
    
    val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), paths.Value.name)

    InnerObjectConcat(wrappedIdentitySpec, wrappedValueSpec)
  }
  
  private def buildIdShuffleSpec(indexes: Vector[Int]): TransSpec1 = {
    indexes map { idx =>
      trans.WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(idx))): TransSpec1
    } reduce { trans.ArrayConcat(_, _) }
  }
  
  private def flip[A, B, C](f: (A, B) => C)(b: B, a: A): C = f(a, b)      // is this in scalaz?
  
  private def liftToValues(trans: TransSpec1): TransSpec1 =
    TableTransSpec.makeTransSpec(Map(paths.Value -> trans))
   
  
  type TableTransSpec[+A <: SourceType] = Map[CPathField, TransSpec[A]]
  type TableTransSpec1 = TableTransSpec[Source1]
  type TableTransSpec2 = TableTransSpec[Source2]
  
  object TableTransSpec {
    def makeTransSpec(tableTrans: TableTransSpec1): TransSpec1 = {
      val wrapped = for ((key @ CPathField(fieldName), value) <- tableTrans) yield {
        val mapped = TransSpec.deepMap(value) {
          case Leaf(_) => DerefObjectStatic(Leaf(Source), key)
        }
        
        trans.WrapObject(mapped, fieldName)
      }
      
      wrapped.foldLeft[TransSpec1](ObjectDelete(Leaf(Source), Set(tableTrans.keys.toSeq: _*))) { (acc, ts) =>
        trans.InnerObjectConcat(acc, ts)
      }
    }
  }

  sealed trait Context {
  }
  
  private case class EvaluatorState(assume: Map[DepGraph, M[Table]] = Map(), extraCount: Int = 0)
  
  private case class PendingTable(table: M[Table], graph: DepGraph, trans: TransSpec1)
}
