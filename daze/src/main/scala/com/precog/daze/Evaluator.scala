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

import akka.dispatch.Future
import akka.util.duration._
import blueeyes.json.JPath

import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.util._
import com.precog.common.{Path, VectorCase}
import com.precog.bytecode._

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._
import collection.immutable.ListSet

import akka.dispatch.{Await, Future}
import akka.util.duration._
import blueeyes.json.{JPathField, JPathIndex}

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect._
import scalaz.syntax.traverse._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.partialFunction._
import scalaz.{StateT, Id, Monoid}
import scalaz.Scalaz._

import com.weiglewilczek.slf4s.Logging

trait EvaluatorConfig {
  implicit def valueSerialization: SortSerialization[SValue]
  implicit def eventSerialization: SortSerialization[(Identities, SValue)]
  implicit def groupSerialization: SortSerialization[(SValue, Identities, SValue)]
  implicit def memoSerialization: IncrementalSerialization[(Identities, SValue)]
  def maxEvalDuration: akka.util.Duration
  def idSource: IdSource
}

trait Evaluator extends DAG
    with CrossOrdering
    with Memoizer
    with TypeInferencer
    with TableModule        // TODO specific implementation
    with ImplLibrary
    with InfixLib
    with UnaryLib
    with BigDecimalOperations
    with YggConfigComponent 
    with Logging { self =>
  
  import Function._
  
  import instructions._
  import dag._
  import trans._

  type YggConfig <: EvaluatorConfig
  
  //private type State[S, T] = StateT[Id, S, T]
  
  private case class EvaluatorState(assume: Map[DepGraph, Future[Table]])

  sealed trait Context

  implicit def asyncContext: akka.dispatch.ExecutionContext

  def withContext[A](f: Context => A): A = 
    f(new Context {})

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def PrimitiveEqualsF2: F2
  def ConstantEmptyArray: F1
  
  def eval(userUID: String, graph: DepGraph, ctx: Context, optimize: Boolean): Future[Table] = {
    logger.debug("Eval for %s = %s".format(userUID, graph))
  
    def resolveTopLevelGroup(spec: BucketSpec, splits: Map[dag.Split, (Table, Int => Table)]): StateT[Id, EvaluatorState, Future[GroupingSpec[Int]]] = spec match {
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
          } yield GroupingUnion(keySpec, keySpec, leftRes, rightRes, GroupKeyAlign.Eq)
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
          } yield GroupingIntersect(keySpec, keySpec, leftRes, rightRes, GroupKeyAlign.Eq)
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
              
              // FIXME if the target has forcing points, targetTrans is insufficient
              //val PendingTable(_, _, targetTrans) = loop(target, assume + (reducedTarget -> resultTargetTable), splits)
                
              state <- get[EvaluatorState]
              trans = loop(target, splits).eval(state.copy(assume = state.assume + (reducedTarget -> resultTargetTable))).trans
              subSpec <- resolveLowLevelGroup(resultTargetTable, reducedTarget, forest, splits)
            } yield {
              resultTargetTable map { resultTargetTable => resultTargetTable.group(trans, id, subSpec) }
            }
          }
          
          case None => sys.error("O NOES!!!")
        }
      }
      
      case UnfixedSolution(_, _) | dag.Extra(_) => sys.error("assertion error")
    }

    //** only used in resolveTopLevelGroup **/
    def resolveLowLevelGroup(commonTable: Future[Table], commonGraph: DepGraph, forest: BucketSpec, splits: Map[dag.Split, (Table, Int => Table)]): StateT[Id, EvaluatorState, GroupKeySpec] = forest match {
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

        } yield GroupKeySpecSource(JPathField(id.toString), pendingTable.trans)
      }
      
      case dag.Extra(graph) => {
        for {
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume + (commonGraph -> commonTable)) }
          pendingTable <- loop(graph, splits)
          _ <- modify[EvaluatorState] { state => state.copy(assume = state.assume - commonGraph) }

        } yield GroupKeySpecSource(JPathField("extra"), trans.Filter(pendingTable.trans, pendingTable.trans))     // TODO constantify -1
      }
      
      case dag.Group(_, _, _) => sys.error("assertion error")
    }

    def loop(graph: DepGraph, splits: Map[dag.Split, (Table, Int => Table)]): StateT[Id, EvaluatorState, PendingTable] = {
      val assumptionCheck: StateT[Id, EvaluatorState, Option[Future[Table]]] = for {
        state <- get[EvaluatorState]
      } yield state.assume.get(graph)
      
      lazy val result: StateT[Id, EvaluatorState, PendingTable] = graph match {
        case s @ SplitParam(_, index) => {
          val (key, _) = splits(s.parent)
          
          val source = trans.DerefObjectStatic(Leaf(Source), JPathField(index.toString))
          val spec = buildConstantWrapSpec(source)
          
          state(PendingTable(Future(key transform spec), graph, TransSpec1.Id))
        }
        
        case s @ SplitGroup(_, index, _) => {
          val (_, map) = splits(s.parent)
          state(PendingTable(Future(map(index)), graph, TransSpec1.Id))
        }
        
        case Root(_, instr) => {
          val table = graph.value collect {
            case SString(str) => ops.constString(Set(CString(str)))
            case SDecimal(d) => ops.constDecimal(Set(CNum(d)))
            case SBoolean(b) => ops.constBoolean(Set(CBoolean(b)))
            case SNull => ops.constNull
            case SObject(map) if map.isEmpty => ops.constEmptyObject
            case SArray(Vector()) => ops.constEmptyArray
          }
          
          val spec = buildConstantWrapSpec(Leaf(Source))
          
          state(PendingTable(Future(table.get.transform(spec)), graph, TransSpec1.Id))
        }
        
        case dag.New(_, parent) => loop(parent, splits)   // TODO John swears this part is easy
        
        case dag.LoadLocal(_, parent, jtpe) => {
          for {
            pendingTable <- loop(parent, splits)
            val back = pendingTable.table flatMap { _ transform liftToValues(pendingTable.trans) load jtpe }
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Morph1(_, m, parent) => {
          for {
            pendingTable <- loop(parent, splits)
            val back = pendingTable.table flatMap { table => m(table.transform(pendingTable.trans)) }
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Morph2(_, m, left, right) => {
          val spec = trans.ArrayConcat(trans.WrapArray(Leaf(SourceLeft)), trans.WrapArray(Leaf(SourceRight)))
          val key = trans.DerefObjectStatic(Leaf(Source), constants.Key)
          
          for {
            pendingTableLeft <- loop(left, splits)
            pendingTableRight <- loop(right, splits)
            
            val back = for {
              leftTable <- pendingTableLeft.table
              val leftResult = leftTable.transform(pendingTableLeft.trans)
              
              rightTable <- pendingTableRight.table
              val rightResult = rightTable.transform(pendingTableRight.trans)
            
              val aligned = m.alignment match {
                case MorphismAlignment.Cross => leftResult.cross(rightResult)(spec)
                case MorphismAlignment.Match => join(leftResult, rightResult)(key, spec)
              }

              result <- m(aligned)
            } yield result 
          } yield PendingTable(back, graph, TransSpec1.Id)
        }
        
        case dag.Distinct(_, parent) =>
          state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))     // TODO
        
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
        
        case r @ dag.Reduce(_, red, parent) => {
          for {
            pendingTable <- loop(parent, splits)
            liftedTrans = liftToValues(pendingTable.trans)
            result = pendingTable.table map { parentTable => red(parentTable.transform(DerefObjectStatic(liftedTrans, constants.Value))) }
            wrapped = result map { _ transform buildConstantWrapSpec(Leaf(Source)) }
          } yield PendingTable(wrapped, graph, TransSpec1.Id)
        }

        //** the original code **//
        //case s @ dag.Split(line, spec, child) => {
        //  val back = for {
        //    grouping <- resolveTopLevelGroup(spec, assume, splits)
        //    
        //    result <- grouper.merge(grouping) { (key: Table, map: Int => Table) =>
        //      val PendingTable(tableF, _, trans) = loop(child, assume, splits + (s -> (key, map)))
        //      tableF map { _ transform liftToValues(trans) }
        //    }
        //  } yield result
        //  
        //  PendingTable(back, graph, TransSpec1.Id)
        //}
        
        case s @ dag.Split(line, spec, child) => {
          val table = for {
            grouping <- resolveTopLevelGroup(spec, splits)
            state <- get[EvaluatorState]
          } yield {
            for {
              grouping2 <- grouping
            
              result <- grouper.merge(grouping2) { (key: Table, map: Int => Table) =>
                val back = for {
                  pendingTable <- loop(child, splits + (s -> (key, map)))
                } yield pendingTable.table map { _ transform liftToValues(pendingTable.trans) }
                
                back.eval(state): Future[Table]
              }
            } yield result
          } 
          table map { PendingTable(_, graph, TransSpec1.Id) }
        }
        
        // VUnion and VIntersect removed, TODO: remove from bytecode
        
        case Join(_, instr @ (IUnion | IIntersect | SetDifference), left, right) =>
            state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))     // TODO
        
        case Join(_, Map2Cross(Eq) | Map2CrossLeft(Eq) | Map2CrossRight(Eq), left, right) if right.value.isDefined => {
          for {
            pendingTable <- loop(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, PrimitiveEqualsF2.partialRight(svalueToCValue(right.value.get))))
        }
        
        case Join(_, Map2Cross(Eq) | Map2CrossLeft(Eq) | Map2CrossRight(Eq), left, right) if left.value.isDefined => {
          for {
            pendingTable <- loop(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, PrimitiveEqualsF2.partialLeft(svalueToCValue(left.value.get))))
        }
        
        case Join(_, Map2Cross(NotEq) | Map2CrossLeft(NotEq) | Map2CrossRight(NotEq), left, right) if right.value.isDefined => {
          for {
            pendingTable <- loop(left, splits)
          } yield {
            val eqTrans = trans.Map1(pendingTable.trans, PrimitiveEqualsF2.partialRight(svalueToCValue(right.value.get)))
            PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(eqTrans, op1(Comp).f1))
          }
        }
        
        case Join(_, Map2Cross(NotEq) | Map2CrossLeft(NotEq) | Map2CrossRight(NotEq), left, right) if left.value.isDefined => {
          for {
            pendingTable <- loop(right, splits)
          } yield {
            val eqTrans = trans.Map1(pendingTable.trans, PrimitiveEqualsF2.partialRight(svalueToCValue(left.value.get)))
            PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(eqTrans, op1(Comp).f1))
          }
        }
        
        case Join(_, Map2Cross(instructions.WrapObject) | Map2CrossLeft(instructions.WrapObject) | Map2CrossRight(instructions.WrapObject), left, right) if left.value.isDefined => {
          left.value match {
            case Some(value @ SString(str)) => {
              //loop(right, splits) map { pendingTable => PendingTable(pendingTable.table, pendingTable.graph, trans.WrapObject(pendingTable.trans, str)) }

              for {
                pendingTable <- loop(right, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.WrapObject(pendingTable.trans, str))
            }
            
            case _ =>
              state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), left, right) if right.value.isDefined => {
          right.value match {
            case Some(value @ SString(str)) => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefObjectStatic(pendingTable.trans, JPathField(str)))
            }
            
            case _ =>
              state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), left, right) if right.value.isDefined => {
          right.value match {
            case Some(value @ SDecimal(d)) => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefArrayStatic(pendingTable.trans, JPathIndex(d.toInt)))
            }
            
            // TODO other numeric types
            
            case _ =>
              state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, Map2Cross(instructions.ArraySwap) | Map2CrossLeft(instructions.ArraySwap) | Map2CrossRight(instructions.ArraySwap), left, right) if right.value.isDefined => {
          right.value match {
            case Some(value @ SDecimal(d)) => {     // TODO other numeric types
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, trans.ArraySwap(pendingTable.trans, d.toInt))
            }
            
            case _ =>
              state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))
          }
        }
  
        // case Join(_, Map2CrossLeft(op), left, right) if right.isSingleton =>
        
        // case Join(_, Map2CrossRight(op), left, right) if left.isSingleton =>
        
        // begin: annoyance with Scala's lousy pattern matcher
        case Join(_, opSpec @ (Map2Cross(_) | Map2CrossRight(_) | Map2CrossLeft(_)), left, right) if right.value.isDefined => {
          val op = opSpec match {
            case Map2Cross(op) => op
            case Map2CrossRight(op) => op
            case Map2CrossLeft(op) => op
          }
          
          val cv = svalueToCValue(right.value.get)
          val f1 = op2(op).f2.partialRight(cv)

          for {
            pendingTable <- loop(left, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, f1))
        }
        
        case Join(_, opSpec @ (Map2Cross(_) | Map2CrossRight(_) | Map2CrossLeft(_)), left, right) if left.value.isDefined => {
          val op = opSpec match {
            case Map2Cross(op) => op
            case Map2CrossRight(op) => op
            case Map2CrossLeft(op) => op
          }
          
          val cv = svalueToCValue(left.value.get)
          val f1 = op2(op).f2.partialLeft(cv)
          
          for {
            pendingTable <- loop(right, splits)
          } yield PendingTable(pendingTable.table, pendingTable.graph, trans.Map1(pendingTable.trans, f1))
        }
        // end: annoyance
        
        case Join(_, Map2Match(op), left, right) => {
          // TODO binary typing

          for {
            pendingTableLeft <- loop(left, splits)
            pendingTableRight <- loop(right, splits)
          } yield {
            if (pendingTableLeft.graph == pendingTableRight.graph) {
              PendingTable(pendingTableLeft.table, pendingTableLeft.graph, transFromBinOp(op)(pendingTableLeft.trans, pendingTableRight.trans))
            } else {
              val key = trans.DerefObjectStatic(Leaf(Source), constants.Key)
              val spec = buildWrappedJoinSpec(sharedPrefixLength(left, right), left.provenance.length, right.provenance.length)(transFromBinOp(op))
              
              val result = for {
                parentLeftTable <- pendingTableLeft.table 
                val leftResult = parentLeftTable.transform(pendingTableLeft.trans)
                
                parentRightTable <- pendingTableRight.table 
                val rightResult = parentRightTable.transform(pendingTableRight.trans)
              } yield join(leftResult, rightResult)(key, spec)
              
              PendingTable(result, graph, TransSpec1.Id)
            } 
          }
        }
  
        // guaranteed: cross, cross_left and cross_right
        case j @ Join(_, instr, left, right) => {
          val (op, isLeft) = instr match {
            case Map2Cross(op) => (op, true)
            case Map2CrossRight(op) => (op, false)
            case Map2CrossLeft(op) => (op, true)
          }
          
          for {
            pendingTableLeft <- loop(left, splits)
            pendingTableRight <- loop(right, splits)
          } yield {
            val result = for {
              parentLeftTable <- pendingTableLeft.table 
              val leftResult = parentLeftTable.transform(pendingTableLeft.trans)
              
              parentRightTable <- pendingTableRight.table 
              val rightResult = parentRightTable.transform(pendingTableRight.trans)
            } yield {
              if (isLeft)
                leftResult.cross(rightResult)(buildWrappedCrossSpec(transFromBinOp(op)))
              else
                rightResult.cross(leftResult)(buildWrappedCrossSpec(flip(transFromBinOp(op))))
            }
            
            PendingTable(result, graph, TransSpec1.Id)
          }
        }
        
        case dag.Filter(_, None, target, boolean) => {
          // TODO binary typing
          
          for {
            pendingTableTarget <- loop(target, splits)
            pendingTableBoolean <- loop(boolean, splits)
          } yield {
            if (pendingTableTarget.graph == pendingTableBoolean.graph)
              PendingTable(pendingTableTarget.table, pendingTableTarget.graph, trans.Filter(pendingTableTarget.trans, pendingTableBoolean.trans))
            else {
              val key = trans.DerefObjectStatic(Leaf(Source), constants.Key)
              
              val spec = buildWrappedJoinSpec(sharedPrefixLength(target, boolean), target.provenance.length, boolean.provenance.length) { (srcLeft, srcRight) =>
                trans.Filter(srcLeft, srcRight)
              }
              
              val result = for {
                parentTargetTable <- pendingTableTarget.table 
                val targetResult = parentTargetTable.transform(pendingTableTarget.trans)
                
                parentBooleanTable <- pendingTableBoolean.table
                val booleanResult = parentBooleanTable.transform(pendingTableBoolean.trans)
              } yield join(targetResult, booleanResult)(key, spec)
              
              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case f @ dag.Filter(_, Some(cross), target, boolean) => {
          val isLeft = cross match {
            case CrossNeutral => true
            case CrossRight => false
            case CrossLeft => true
          }
          
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
              val targetResult = parentTargetTable.transform(pendingTableTarget.trans)
              
              parentBooleanTable <- pendingTableBoolean.table
              val booleanResult = parentBooleanTable.transform(pendingTableBoolean.trans)
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
        
        case s @ Sort(parent, indexes) =>
          state(PendingTable(Future(ops.empty), graph, TransSpec1.Id))     // TODO
        
        case m @ Memoize(parent, _) =>
          loop(parent, splits)     // TODO
      }
      
      assumptionCheck flatMap { assumedResult: Option[Future[Table]] =>
        val liftedAssumption = assumedResult map { table =>
          state[EvaluatorState, PendingTable](
            PendingTable(table, graph, TransSpec1.Id))
        }
        
        liftedAssumption getOrElse result
      }
    }
    
    val rewrite = (orderCrosses _) andThen
      (memoize _) andThen
      (if (optimize) inferTypes(JType.JUnfixedT) else identity)

    val resultState: StateT[Id, EvaluatorState, Future[Table]] = 
      loop(rewrite(graph), Map()) map { pendingTable => pendingTable.table map { _ transform liftToValues(pendingTable.trans) } }

    resultState.eval(EvaluatorState(Map()))

    //** original code **//
    //val PendingTable(table, _, spec) = loop(rewrite(graph), Map())
    //table map { _ transform liftToValues(spec) }
  }
  
  private def findCommonality(forest: Set[DepGraph]): Option[DepGraph] = {
    val sharedPrefixReversed = forest flatMap buildChains map { _.reverse } reduceOption { (left, right) =>
      left zip right takeWhile { case (a, b) => a == b } map { _._1 }
    }
    
    sharedPrefixReversed flatMap { _.lastOption }
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
    
    parts reduce { (left, right) => trans.ObjectConcat(left, right) }
  }
  
  private def buildChains(graph: DepGraph): Set[List[DepGraph]] =
    enumerateParents(graph) flatMap buildChains map { graph :: _ }
  
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
    case dag.Split(_, spec, _) => enumerateGraphs(spec)
    case Join(_, _, left, right) => Set(left, right)
    case dag.Filter(_, _, target, boolean) => Set(target, boolean)
    case dag.Sort(parent, _) => Set(parent)
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
    case JoinObject => ObjectConcat(left, right)
    case JoinArray => ArrayConcat(left, right)
    case instructions.ArraySwap => sys.error("nothing happens")
    case DerefObject => DerefObjectDynamic(left, right)
    case DerefArray => DerefArrayDynamic(left, right)
    case _ => trans.Map2(left, right, op2(op).f2)
  }

  private def sharedPrefixLength(left: DepGraph, right: DepGraph): Int =
    left.provenance zip right.provenance takeWhile { case (a, b) => a == b } length
  
  private def svalueToCValue(sv: SValue) = sv match {
    case SString(str) => CString(str)
    case SDecimal(d) => CNum(d)
    // case SLong(l) => CLong(l)
    // case SDouble(d) => CDouble(d)
    case SNull => CNull
    case SObject(obj) if obj.isEmpty => CEmptyObject
    case SArray(Vector()) => CEmptyArray
    case _ => sys.error("die a horrible death")
  }
  
  private def join(left: Table, right: Table)(key: TransSpec1, spec: TransSpec2): Table = {
    val emptySpec = trans.Map1(Leaf(Source), ConstantEmptyArray)
    val result = left.cogroup(key, key, right)(emptySpec, emptySpec, trans.WrapArray(spec))
    result.transform(trans.DerefArrayStatic(Leaf(Source), JPathIndex(0)))
  }
  
  private def buildConstantWrapSpec[A <: SourceType](source: TransSpec[A]): TransSpec[A] = {
    val bottomWrapped = trans.WrapObject(trans.Map1(source, ConstantEmptyArray), constants.Key.name)
    trans.ObjectConcat(bottomWrapped, trans.WrapObject(source, constants.Value.name))
  }
  
  private def buildWrappedJoinSpec(sharedLength: Int, leftLength: Int, rightLength: Int)(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
    val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), constants.Key)
    val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), constants.Key)
    
    val sharedDerefs = for (i <- 0 until sharedLength)
      yield DerefArrayStatic(leftIdentitySpec, JPathIndex(i))
    
    val unsharedLeft = for (i <- (sharedLength - 1) until (leftLength - sharedLength))
      yield DerefArrayStatic(leftIdentitySpec, JPathIndex(i))
    
    val unsharedRight = for (i <- (sharedLength - 1) until (rightLength - sharedLength))
      yield DerefArrayStatic(rightIdentitySpec, JPathIndex(i))
    
    val derefs: Seq[TransSpec2] = sharedDerefs ++ unsharedLeft ++ unsharedRight
    
    val newIdentitySpec = if (derefs.isEmpty)
      trans.Map1(Leaf(SourceLeft), ConstantEmptyArray)
    else
      derefs reduce { trans.ArrayConcat(_, _) }
    
    val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, constants.Key.name)
    
    val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), constants.Value)
    val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), constants.Value)
    
    val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), constants.Value.name)
      
    ObjectConcat(
      ObjectConcat(
        ObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)),
        wrappedIdentitySpec),
      wrappedValueSpec)
  }
  
  private def buildWrappedCrossSpec(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
    val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), constants.Key)
    val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), constants.Key)
    
    val newIdentitySpec = ArrayConcat(leftIdentitySpec, rightIdentitySpec)
    
    val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, constants.Key.name)
    
    val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), constants.Value)
    val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), constants.Value)
    
    val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), constants.Value.name)
      
    ObjectConcat(
      ObjectConcat(
        ObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)),
        wrappedIdentitySpec),
      wrappedValueSpec)
  }
  
  private def flip[A, B, C](f: (A, B) => C)(b: B, a: A): C = f(a, b)      // is this in scalaz?
  
  private def liftToValues(trans: TransSpec1): TransSpec1 =
    TableTransSpec.makeTransSpec(Map(constants.Value -> trans))
   
  
  private type TableTransSpec[+A <: SourceType] = Map[JPathField, TransSpec[A]]
  private type TableTransSpec1 = TableTransSpec[Source1]
  private type TableTransSpec2 = TableTransSpec[Source2]
  
  private object TableTransSpec {
    def makeTransSpec(tableTrans: TableTransSpec1): TransSpec1 = {
      val wrapped = for ((key @ JPathField(fieldName), value) <- tableTrans) yield {
        val mapped = deepMap(value) {
          case lf @ Leaf(_) =>
            DerefObjectStatic(lf, key)
        }
        
        trans.WrapObject(mapped, fieldName)
      }
      
      wrapped.foldLeft(ObjectDelete(Leaf(Source), Set(tableTrans.keys.toSeq: _*)): TransSpec1) { (acc, ts) =>
        trans.ObjectConcat(acc, ts)
      }
    }
    
    private def deepMap(spec: TransSpec1)(f: PartialFunction[TransSpec1, TransSpec1]): TransSpec1 = spec match {
      case x if f isDefinedAt x => f(x)
      case x @ Leaf(_) => x
      case trans.Filter(source, pred) => trans.Filter(deepMap(source)(f), deepMap(pred)(f))
      case Scan(source, scanner) => Scan(deepMap(source)(f), scanner)
      case trans.Map1(source, f1) => trans.Map1(deepMap(source)(f), f1)
      case trans.Map2(left, right, f2) => trans.Map2(deepMap(left)(f), deepMap(right)(f), f2)
      case trans.ObjectConcat(left, right) => trans.ObjectConcat(deepMap(left)(f), deepMap(right)(f))
      case trans.ArrayConcat(left, right) => trans.ArrayConcat(deepMap(left)(f), deepMap(right)(f))
      case trans.WrapObject(source, field) => trans.WrapObject(deepMap(source)(f), field)
      case trans.WrapArray(source) => trans.WrapArray(deepMap(source)(f))
      case DerefObjectStatic(source, field) => DerefObjectStatic(deepMap(source)(f), field)
      case DerefObjectDynamic(left, right) => DerefObjectDynamic(deepMap(left)(f), deepMap(right)(f))
      case DerefArrayStatic(source, element) => DerefArrayStatic(deepMap(source)(f), element)
      case DerefArrayDynamic(left, right) => DerefArrayDynamic(deepMap(left)(f), deepMap(right)(f))
      case trans.ArraySwap(source, index) => trans.ArraySwap(deepMap(source)(f), index)
      case Typed(source, tpe) => Typed(deepMap(source)(f), tpe)
      case trans.Equal(left, right) => trans.Equal(deepMap(left)(f), deepMap(right)(f))
    }
  }
  
  private case class PendingTable(table: Future[Table], graph: DepGraph, trans: TransSpec1)
}
