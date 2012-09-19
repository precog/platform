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

import blueeyes.json.JPath

import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.util.IdSourceConfig
import com.precog.util._
import com.precog.common.{Path, VectorCase}
import com.precog.bytecode._

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._
import collection.immutable.ListSet

import akka.dispatch.Future

import blueeyes.json.{JPathField, JPathIndex}

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
}

trait Evaluator[M[+_]] extends DAG
    with CrossOrdering
    with Memoizer
    with TypeInferencer
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
  
  def withContext[A](f: Context => A): A = 
    withMemoizationContext { ctx =>
      f(new Context {
          val memoizationContext = ctx
        })
      }

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def PrimitiveEqualsF2: F2
  def ConstantEmptyArray: F1
  
  def freshIdScanner: Scanner

  def rewriteDAG(optimize: Boolean): DepGraph => DepGraph = {
    (orderCrosses _) andThen
    (if (false && optimize) (g => megaReduce(g, findReductions(g))) else identity) andThen
    (if (optimize) inferTypes(JType.JUnfixedT) else identity) andThen
    (if (optimize) (memoize _) else identity)
  }
  
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
              pendingTargetTable <- loop(reducedTarget, splits)
            //val PendingTable(reducedTargetTableF, _, reducedTargetTrans) = loop(reducedTarget, assume, splits)
           
              resultTargetTable = pendingTargetTable.table map { _.transform { liftToValues(pendingTargetTable.trans) } }
              
              // TODO FIXME if the target has forcing points, targetTrans is insufficient
              //val PendingTable(_, _, targetTrans) = loop(target, assume + (reducedTarget -> resultTargetTable), splits)
                
              state <- get[EvaluatorState]
              trans = loop(target, splits).eval(state.copy(assume = state.assume + (reducedTarget -> resultTargetTable))).trans
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
          pendingTable <- loop(solution, splits)
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume - commonGraph) }

          liftedTrans = TransSpec.deepMap(pendingTable.trans) {
            case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
          }
        } yield GroupKeySpecSource(JPathField(id.toString), liftedTrans)
      }
      
      case dag.Extra(graph) => {
        for {
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (commonGraph -> commonTable)) }
          pendingTable <- loop(graph, splits)
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume - commonGraph) }
          
          state <- get[EvaluatorState]
          extraId = state.extraCount
          _ <- modify[EvaluatorState] { _.copy(extraCount = extraId + 1) }

          liftedTrans = TransSpec.deepMap(pendingTable.trans) {
            case Leaf(_) => DerefObjectStatic(Leaf(Source), paths.Value)
          }
        } yield GroupKeySpecSource(JPathField("extra" + extraId), trans.Filter(liftedTrans, liftedTrans))
      }
      
      case dag.Group(_, _, _) => sys.error("assertion error")
    }

    lazy val reductions: Map[DepGraph, NEL[dag.Reduce]] = findReductions(graph)

    def loop(graph: DepGraph, splits: Map[dag.Split, (Table, Int => M[Table])]): StateT[Id, EvaluatorState, PendingTable] = {
      logger.trace("Loop on %s".format(graph))
      
      val assumptionCheck: StateT[Id, EvaluatorState, Option[M[Table]]] = for {
        state <- get[EvaluatorState]
      } yield state.assume.get(graph)
      
      def result: StateT[Id, EvaluatorState, PendingTable] = graph match {
        case s @ SplitParam(_, index) => {
          val (key, _) = splits(s.parent)
          
          val source = trans.DerefObjectStatic(Leaf(Source), JPathField(index.toString))
          val spec = buildConstantWrapSpec(source)
          
          state(PendingTable(M.point(key transform spec), graph, TransSpec1.Id))
        }
        
        case s @ SplitGroup(_, index, _) => {
          val (_, f) = splits(s.parent)
          state(PendingTable(f(index), graph, TransSpec1.Id))
        }
        
        case Root(_, instr) => {
          val table = graph.value collect {
            case SString(str) => Table.constString(Set(CString(str)))
            case SDecimal(d) => Table.constDecimal(Set(CNum(d)))
            case SBoolean(b) => Table.constBoolean(Set(CBoolean(b)))
            case SNull => Table.constNull
            case SObject(map) if map.isEmpty => Table.constEmptyObject
            case SArray(Vector()) => Table.constEmptyArray
          }
          
          val spec = buildConstantWrapSpec(Leaf(Source))
          
          state(PendingTable(M.point(table.get.transform(spec)), graph, TransSpec1.Id))
        }
        
        // TODO technically, we can do this without forcing by pre-lifting PendingTable#trans
        case dag.New(_, parent) => {
          for {
            pendingTable <- loop(parent, splits)
            spec = TableTransSpec.makeTransSpec(
              Map(paths.Key -> trans.WrapArray(Scan(Leaf(Source), freshIdScanner))))
            
            tableM2 = for {
              table <- pendingTable.table
              transformed = table.transform(liftToValues(pendingTable.trans))  //TODO `transformed` is not used
            } yield table.transform(spec)
          } yield PendingTable(tableM2, graph, TransSpec1.Id)
        }
        
        case dag.LoadLocal(_, parent, jtpe) => {
          for {
            pendingTable <- loop(parent, splits)
            Path(prefixStr) = prefix
            f1 = Infix.concatString.f2.partialLeft(CString(prefixStr.replaceAll("/$", "")))
            trans2 = trans.Map1(trans.DerefObjectStatic(pendingTable.trans, paths.Value), f1)
            val back = pendingTable.table flatMap { _.transform(trans2).load(userUID, jtpe) }
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Morph1(_, mor, parent) => {
          for {
            pendingTable <- loop(parent, splits)
            val back = pendingTable.table flatMap { table => mor(table.transform(liftToValues(pendingTable.trans))) }
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Morph2(_, mor, left, right) => {
          lazy val spec = trans.ArrayConcat(trans.WrapArray(Leaf(SourceLeft)), trans.WrapArray(Leaf(SourceRight)))
          lazy val specRight = trans.ArrayConcat(trans.WrapArray(Leaf(SourceRight)), trans.WrapArray(Leaf(SourceLeft)))
          lazy val key = trans.DerefObjectStatic(Leaf(Source), paths.Key)
          
          for {
            pendingTableLeft <- loop(left, splits)
            pendingTableRight <- loop(right, splits)
            
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
              leftSpec = DerefObjectStatic(DerefArrayStatic(TransSpec1.Id, JPathIndex(0)), paths.Value)
              rightSpec = DerefObjectStatic(DerefArrayStatic(TransSpec1.Id, JPathIndex(1)), paths.Value)
              transformed = aligned.transform(ArrayConcat(trans.WrapArray(leftSpec), trans.WrapArray(rightSpec)))

              result <- mor(transformed)
            } yield result

          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Distinct(_, parent) => {
          for {
            pending <- loop(parent, splits)
          } yield {
            val keySpec   = DerefObjectStatic(Leaf(Source), paths.Key)
            val valueSpec = DerefObjectStatic(Leaf(Source), paths.Value)
            
            val result = for {
              pendingTable <- pending.table
              table = pendingTable.transform(liftToValues(pending.trans))
              sorted <- table.sort(valueSpec, SortAscending)
              distinct = sorted.distinct(valueSpec) 
              identitySorted <- distinct.sort(keySpec, SortAscending) 
            } yield {
              identitySorted
            }
            PendingTable(result, graph, TransSpec1.Id)
          }
        }

        case Operate(_, instructions.WrapArray, parent) => {
          for {
            pendingTable <- loop(parent, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.WrapArray(pendingTable.trans))
        }
        
        case o @ Operate(_, op, parent) => {
          for {
            pendingTable <- loop(parent, splits)
            
            // TODO unary typing
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, op1(op).f1))
        }

        /**
        returns an array (to be dereferenced later) containing the result of each reduction
        */
        case m @ MegaReduce(_, reds, parent) => {
          val red: ReductionImpl = coalesce(reds map { _.red })  

          for {
            pendingTable <- loop(parent, splits)
            liftedTrans = liftToValues(pendingTable.trans)

            result = pendingTable.table flatMap { parentTable => red(parentTable.transform(DerefObjectStatic(liftedTrans, paths.Value))) }
            keyWrapped = trans.WrapObject(trans.ConstLiteral(CEmptyArray, trans.DerefArrayStatic(Leaf(Source), JPathIndex(0))), paths.Key.name)  //TODO deref by index 0 is WRONG
            valueWrapped = trans.InnerObjectConcat(keyWrapped, trans.WrapObject(Leaf(Source), paths.Value.name))
            wrapped = result map { _ transform valueWrapped }
            _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (m -> wrapped)) }
          } yield {
            PendingTable(wrapped, graph, TransSpec1.Id)
          }
        }
        
        case r @ dag.Reduce(_, red, parent) => {
          for {
            pendingTable <- loop(parent, splits)
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
                val back = for {
                  pending <- loop(child, splits + (s -> (key -> map)))
                } yield {
                  for {
                    pendingTable <- pending.table
                  } yield pendingTable.transform(liftToValues(pending.trans))
                }

                back.eval(state)  //: M[Table]
              }
            } yield result.transform(idSpec)
          } 
          table map { PendingTable(_, graph, TransSpec1.Id) }
        }
        
        case IUI(_, union, left, right) => {
          for {
            leftPending <- loop(left, splits)
            rightPending <- loop(right, splits)
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
            leftPending <- loop(left, splits)
            rightPending <- loop(right, splits)
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
        
        case Join(_, Eq, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          for {
            pendingTable <- loop(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, svalueToCValue(right.value.get), false))
        }
        
        case Join(_, Eq, CrossLeftSort | CrossRightSort, left, right) if left.value.isDefined => {
          for {
            pendingTable <- loop(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, svalueToCValue(left.value.get), false))
        }
        
        case Join(_, NotEq, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          for {
            pendingTable <- loop(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, svalueToCValue(right.value.get), true))
        }
        
        case Join(_, NotEq, CrossLeftSort | CrossRightSort, left, right) if left.value.isDefined => {
          for {
            pendingTable <- loop(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.EqualLiteral(pendingTable.trans, svalueToCValue(left.value.get), true))
        }
        
        case Join(_, instructions.WrapObject, CrossLeftSort | CrossRightSort, left, right) if left.value.isDefined => {
          left.value match {
            case Some(value @ SString(str)) => {
              //loop(right, splits) map { pendingTable => PendingTable(pendingTable.table, pendingTable.graph, trans.WrapObject(pendingTable.trans, str)) }

              for {
                pendingTable <- loop(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.WrapObject(pendingTable.trans, str))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, DerefObject, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          right.value match {
            case Some(value @ SString(str)) => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefObjectStatic(pendingTable.trans, JPathField(str)))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, DerefArray, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          right.value match {
            case Some(SDecimal(d)) => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefArrayStatic(pendingTable.trans, JPathIndex(d.toInt)))
            }
            
            // TODO other numeric types
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.ArraySwap, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          right.value match {
            case Some(value @ SDecimal(d)) => {     // TODO other numeric types
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArraySwap(pendingTable.trans, d.toInt))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinObject, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          right.value match {
            case Some(SObject(obj)) if obj.isEmpty => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.InnerObjectConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinObject, CrossLeftSort | CrossRightSort, left, right) if left.value.isDefined => {
          left.value match {
            case Some(SObject(obj)) if obj.isEmpty => {
              for {
                pendingTable <- loop(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.InnerObjectConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinArray, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          right.value match {
            case Some(SObject(obj)) if obj.isEmpty => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArrayConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, instructions.JoinArray, CrossLeftSort | CrossRightSort, left, right) if left.value.isDefined => {
          left.value match {
            case Some(SObject(obj)) if obj.isEmpty => {
              for {
                pendingTable <- loop(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArrayConcat(pendingTable.trans))
            }
            
            case _ =>
              state(PendingTable(M.point(Table.empty), graph, TransSpec1.Id))
          }
        }
  
        // case Join(_, Map2CrossLeft(op), left, right) if right.isSingleton =>
        
        // case Join(_, Map2CrossRight(op), left, right) if left.isSingleton =>
        
        // begin: annoyance with Scala's lousy pattern matcher
        case Join(_, op, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          val cv = svalueToCValue(right.value.get)
          val f1 = op2(op).f2.partialRight(cv)

          for {
            pendingTable <- loop(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, f1))
        }
        
        case Join(_, op, CrossLeftSort | CrossRightSort, left, right) if left.value.isDefined => {
          val cv = svalueToCValue(left.value.get)
          val f1 = op2(op).f2.partialLeft(cv)
          
          for {
            pendingTable <- loop(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, f1))
        }
        // end: annoyance
        
        case Join(_, op, joinSort @ (IdentitySort | ValueSort(_)), left, right) => {
          // TODO binary typing

          for {
            pendingTableLeft <- loop(left, splits)
            pendingTableRight <- loop(right, splits)
          } yield {

            if (pendingTableLeft.graph == pendingTableRight.graph) {
              PendingTable(pendingTableLeft.table, pendingTableLeft.graph, transFromBinOp(op)(pendingTableLeft.trans, pendingTableRight.trans))
            } else {
              val prefixLength = sharedPrefixLength(left, right)
              
              val key = joinSort match {
                case IdentitySort =>
                  buildJoinKeySpec(prefixLength)
                
                case ValueSort(id) =>
                  trans.DerefObjectStatic(Leaf(Source), JPathField("sort-" + id))
                
                case _ => sys.error("unreachable code")
              }
              
              val spec = buildWrappedJoinSpec(prefixLength, left.identities.length, right.identities.length)(transFromBinOp(op))

              val result = for {
                parentLeftTable <- pendingTableLeft.table
                val leftResult = parentLeftTable.transform(liftToValues(pendingTableLeft.trans))
                
                parentRightTable <- pendingTableRight.table
                val rightResult = parentRightTable.transform(liftToValues(pendingTableRight.trans))

              } yield join(leftResult, rightResult)(key, spec)

              PendingTable(result, graph, TransSpec1.Id)
            } 
          }
        }
  
        case j @ Join(_, op, joinSort @ (CrossLeftSort | CrossRightSort), left, right) => {
          val isLeft = joinSort == CrossLeftSort

          for {
            pendingTableLeft <- loop(left, splits)
            pendingTableRight <- loop(right, splits)
          } yield {
            val result = for {
              parentLeftTable <- pendingTableLeft.table 
              val leftResult = parentLeftTable.transform(liftToValues(pendingTableLeft.trans))
              
              parentRightTable <- pendingTableRight.table 
              val rightResult = parentRightTable.transform(liftToValues(pendingTableRight.trans))
            } yield {
              if (isLeft)
                leftResult.cross(rightResult)(buildWrappedCrossSpec(transFromBinOp(op)))
              else
                rightResult.cross(leftResult)(buildWrappedCrossSpec(flip(transFromBinOp(op))))
            }
            
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        case dag.Filter(_, joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => {
          // TODO binary typing

          for {
            pendingTableTarget <- loop(target, splits)
            pendingTableBoolean <- loop(boolean, splits)
          } yield {
            if (pendingTableTarget.graph == pendingTableBoolean.graph) {
              PendingTable(pendingTableTarget.table, pendingTableTarget.graph, trans.Filter(pendingTableTarget.trans, pendingTableBoolean.trans))
            } else {
              val key = joinSort match {
                case IdentitySort =>
                  trans.DerefObjectStatic(Leaf(Source), paths.Key)
                
                case ValueSort(id) =>
                  trans.DerefObjectStatic(Leaf(Source), JPathField("sort-" + id))
                
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
            pendingTableTarget <- loop(target, splits)
            pendingTableBoolean <- loop(boolean, splits)
          } yield {
            val result = for {
              parentTargetTable <- pendingTableTarget.table 
              val targetResult = parentTargetTable.transform(liftToValues(pendingTableTarget.trans))
              
              parentBooleanTable <- pendingTableBoolean.table
              val booleanResult = parentBooleanTable.transform(liftToValues(pendingTableBoolean.trans))
            } yield {
              if (isLeft) {
                val spec = buildWrappedCrossSpec { (srcLeft, srcRight) =>
                  trans.Filter(srcLeft, srcRight)
                }
                targetResult.cross(booleanResult)(spec)
              } else {
                val spec = buildWrappedCrossSpec { (srcLeft, srcRight) =>
                  trans.Filter(srcRight, srcLeft)
                }
                booleanResult.cross(targetResult)(spec)
              }
            }
            
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        case s @ Sort(parent, indexes) => {
          if (indexes == Vector(0 until indexes.length: _*) && parent.sorting == IdentitySort) {
            loop(parent, splits)
          } else {
            val fullOrder = indexes ++ ((0 until parent.identities.length) filterNot (indexes contains))
            val idSpec = buildIdShuffleSpec(fullOrder)
            
            for {
              pending <- loop(parent, splits)
            } yield {
              val sortedResult = for {
                pendingTable <- pending.table
                val table = pendingTable.transform(liftToValues(pending.trans))
                val shuffled = table.transform(TableTransSpec.makeTransSpec(Map(paths.Key -> idSpec)))
                // TODO this could be made more efficient by only considering the indexes we care about
                sorted <- shuffled.sort(DerefObjectStatic(Leaf(Source), paths.Key), SortAscending)
              } yield {                              
                parent.sorting match {
                  case ValueSort(id) =>
                    sorted.transform(ObjectDelete(Leaf(Source), Set(JPathField("sort-" + id))))
                    
                  case _ => sorted
                }
              }
              
              PendingTable(sortedResult, graph, TransSpec1.Id)
            }
          }
        }
        
        case s @ SortBy(parent, sortField, valueField, id) => {
          val sortSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), JPathField(sortField))
          val valueSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), JPathField(valueField))
                
          if (parent.sorting == ValueSort(id)) {
            loop(parent, splits)
          } else {
            for {
              pending <- loop(parent, splits)
            } yield {
              val result = for {
                pendingTable <- pending.table
                table = pendingTable.transform(liftToValues(pending.trans))
                sorted <- table.sort(sortSpec, SortAscending)
              } yield {
                val wrappedSort = trans.WrapObject(sortSpec, "sort-" + id)
                val wrappedValue = trans.WrapObject(valueSpec, paths.Value.name)
                
                val oldSortField = parent.sorting match {
                  case ValueSort(id2) if id != id2 =>
                    Some(JPathField("sort-" + id2))
                  
                  case _ => None
                }
                
                val spec = InnerObjectConcat(
                  InnerObjectConcat(
                    ObjectDelete(Leaf(Source), Set(JPathField("sort-" + id), paths.Value) ++ oldSortField),
                      wrappedSort),
                      wrappedValue)
                
                sorted.transform(spec)
              }
              
              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case s @ ReSortBy(parent, id) => {
          if (parent.sorting == ValueSort(id)) {
            loop(parent, splits)
          } else {
            for {
              pending <- loop(parent, splits)
            } yield {
              val result = for {
                pendingTable <- pending.table
                val table = pendingTable.transform(liftToValues(pending.trans))
                sorted <- table.sort(DerefObjectStatic(DerefObjectStatic(Leaf(Source), paths.Value), JPathField("sort-" + id)), SortAscending)
              } yield sorted
              
              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case Memoize(parent, memoId) =>
          for {
            pending <- loop(parent, splits)
          } yield {
            val result = for {
              pendingTable <- pending.table
              table = pendingTable.transform(liftToValues(pending.trans))
              memoized <- ctx.memoizationContext.memoize(table, memoId)
            } yield memoized
            
            PendingTable(result, graph, TransSpec1.Id)
          }
      }

      assumptionCheck flatMap { assumedResult: Option[M[Table]] =>
        val liftedAssumption = assumedResult map { table =>
          state[EvaluatorState, PendingTable](
            PendingTable(table, graph, TransSpec1.Id))
        }
        
        liftedAssumption getOrElse result
      }
    }
    
    val resultState: StateT[Id, EvaluatorState, M[Table]] = 
      loop(rewriteDAG(optimize)(graph), Map()) map { pendingTable => pendingTable.table map { _ transform liftToValues(pendingTable.trans) } }

    resultState.eval(EvaluatorState(Map()))
  }
  
  /**
   * Returns all `Memoize` nodes in the graph, ordered topologically.
   */
  private def listMemos(queue: Queue[DepGraph], acc: List[dag.Memoize] = Nil): List[dag.Memoize] = {
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
      
      val (queue3, addend) = graph match {
        case _: dag.SplitParam => (queue2, None)
        case _: dag.SplitGroup => (queue2, None)
        case _: dag.Root => (queue2, None)
        
        case dag.New(_, parent) => (queue2 enqueue parent, None)
        
        case dag.Morph1(_, _, parent) => (queue2 enqueue parent, None)
        case dag.Morph2(_, _, left, right) => (queue2 enqueue left enqueue right, None)
        
        case dag.Distinct(_, parent) => (queue2 enqueue parent, None)
        
        case dag.LoadLocal(_, parent, _) => (queue2 enqueue parent, None)
        
        case dag.Operate(_, _, parent) => (queue2 enqueue parent, None)
        
        case dag.Reduce(_, _, parent) => (queue2 enqueue parent, None)
        case dag.MegaReduce(_, _, parent) => (queue2 enqueue parent, None)
        
        case dag.Split(_, specs, child) => (queue2 enqueue child enqueue listParents(specs), None)
        
        case dag.IUI(_, _, left, right) => (queue2 enqueue left enqueue right, None)
        case dag.Diff(_, left, right) => (queue2 enqueue left enqueue right, None)
        
        case dag.Join(_, _, _, left, right) => (queue2 enqueue left enqueue right, None)
        case dag.Filter(_, _, left, right) => (queue2 enqueue left enqueue right, None)
        
        case dag.Sort(parent, _) => (queue2 enqueue parent, None)
        case dag.SortBy(parent, _, _, _) => (queue2 enqueue parent, None)
        case dag.ReSortBy(parent, _) => (queue2 enqueue parent, None)
        
        case node @ dag.Memoize(parent, _) => (queue2 enqueue parent, Some(node))
      }
      
      listMemos(queue3, addend map { _ :: acc } getOrElse acc)
    }
  }
  
  private def referencesOnlySplit(graph: DepGraph, split: Option[dag.Split]): Boolean = {
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
      trans.WrapObject(DerefObjectStatic(Leaf(Source), JPathField(id.toString)), id.toString)
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
  
  private def op2(op: BinaryOperation): Op2 = op match {
    case BuiltInFunction2Op(op2) => op2
    
    case instructions.Add => Infix.Add
    case instructions.Sub => Infix.Sub
    case instructions.Mul => Infix.Mul
    case instructions.Div => Infix.Div
    case instructions.Mod => Infix.Mod
    
    case instructions.Lt => Infix.Lt
    case instructions.LtEq => Infix.LtEq
    case instructions.Gt => Infix.Gt
    case instructions.GtEq => Infix.GtEq
    
    case instructions.Eq | instructions.NotEq => sys.error("assertion error")
    
    case instructions.Or => Infix.Or
    case instructions.And => Infix.And
    
    case instructions.WrapObject | instructions.JoinObject |
         instructions.JoinArray | instructions.ArraySwap |
         instructions.DerefObject | instructions.DerefArray => sys.error("assertion error")
  }
  
  private def transFromBinOp[A <: SourceType](op: BinaryOperation)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
    case Eq => trans.Equal(left, right)
    case NotEq => trans.Map1(trans.Equal(left, right), op1(Comp).f1)
    case instructions.WrapObject => WrapObjectDynamic(left, right)
    case JoinObject => InnerObjectConcat(left, right)
    case JoinArray => ArrayConcat(left, right)
    case instructions.ArraySwap => sys.error("nothing happens")
    case DerefObject => DerefObjectDynamic(left, right)
    case DerefArray => DerefArrayDynamic(left, right)
    case _ => trans.Map2(left, right, op2(op).f2)
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

    result.transform(trans.DerefArrayStatic(Leaf(Source), JPathIndex(0)))
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
      yield trans.WrapArray(DerefArrayStatic(SourceKey.Single, JPathIndex(i))): TransSpec1
    
    components reduce { trans.ArrayConcat(_, _) }
  }
  
  private def buildWrappedJoinSpec(sharedLength: Int, leftLength: Int, rightLength: Int)(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
    val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
    val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)
    
    val sharedDerefs = for (i <- 0 until sharedLength)
      yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, JPathIndex(i)))
    
    val unsharedLeft = for (i <- sharedLength until leftLength)
      yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, JPathIndex(i)))
    
    val unsharedRight = for (i <- sharedLength until rightLength)
      yield trans.WrapArray(DerefArrayStatic(rightIdentitySpec, JPathIndex(i)))
    
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
      trans.WrapArray(DerefArrayStatic(Leaf(Source), JPathIndex(idx))): TransSpec1
    } reduce { trans.ArrayConcat(_, _) }
  }
  
  private def flip[A, B, C](f: (A, B) => C)(b: B, a: A): C = f(a, b)      // is this in scalaz?
  
  private def liftToValues(trans: TransSpec1): TransSpec1 =
    TableTransSpec.makeTransSpec(Map(paths.Value -> trans))
   
  
  type TableTransSpec[+A <: SourceType] = Map[JPathField, TransSpec[A]]
  type TableTransSpec1 = TableTransSpec[Source1]
  type TableTransSpec2 = TableTransSpec[Source2]
  
  object TableTransSpec {
    def makeTransSpec(tableTrans: TableTransSpec1): TransSpec1 = {
      val wrapped = for ((key @ JPathField(fieldName), value) <- tableTrans) yield {
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
    def memoizationContext: MemoContext
  }
  
  private case class EvaluatorState(assume: Map[DepGraph, M[Table]] = Map(), extraCount: Int = 0)
  
  private case class PendingTable(table: M[Table], graph: DepGraph, trans: TransSpec1)
}
