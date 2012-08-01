package com.precog
package daze

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

import com.weiglewilczek.slf4s.Logging

trait EvaluatorConfig {
  implicit def valueSerialization: SortSerialization[SValue]
  implicit def eventSerialization: SortSerialization[(Identities, SValue)]
  implicit def groupSerialization: SortSerialization[(SValue, Identities, SValue)]
  implicit def memoSerialization: IncrementalSerialization[(Identities, SValue)]
  def maxEvalDuration: akka.util.Duration
  def idSource: IdSource
}

trait Evaluator[M[+_]] extends DAG
    with CrossOrdering
    with Memoizer
    with TypeInferencer
    with TableModule[M]        // TODO specific implementation
    with ImplLibrary[M]
    with InfixLib[M]
    with UnaryLib[M]
    with BigDecimalOperations
    with YggConfigComponent 
    with Logging { self =>
  
  import Function._
  
  import instructions._
  import dag._
  import trans._

  type YggConfig <: EvaluatorConfig
  
  //private type State[S, T] = StateT[Id, S, T]

  def withContext[A](f: Context => A): A = 
    f(new Context {})

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def PrimitiveEqualsF2: F2
  def ConstantEmptyArray: F1
  
  def freshIdScanner: Scanner
  
  def eval(userUID: String, graph: DepGraph, ctx: Context, optimize: Boolean): M[Table] = {
    logger.debug("Eval for %s = %s".format(userUID, graph))
  
    def resolveTopLevelGroup(spec: BucketSpec, splits: Map[dag.Split, (Table, Int => Table)]): StateT[Id, EvaluatorState, M[GroupingSpec[Int]]] = spec match {
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
    def resolveLowLevelGroup(commonTable: M[Table], commonGraph: DepGraph, forest: BucketSpec, splits: Map[dag.Split, (Table, Int => Table)]): StateT[Id, EvaluatorState, GroupKeySpec] = forest match {
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
          
          state <- get[EvaluatorState]
          extraId = state.extraCount
          _ <- modify[EvaluatorState] { _.copy(extraCount = extraId + 1) }
        } yield GroupKeySpecSource(JPathField("extra" + extraId), trans.Filter(pendingTable.trans, pendingTable.trans))
      }
      
      case dag.Group(_, _, _) => sys.error("assertion error")
    }

    def loop(graph: DepGraph, splits: Map[dag.Split, (Table, Int => Table)]): StateT[Id, EvaluatorState, PendingTable] = {
      val assumptionCheck: StateT[Id, EvaluatorState, Option[M[Table]]] = for {
        state <- get[EvaluatorState]
      } yield state.assume.get(graph)
      
      lazy val result: StateT[Id, EvaluatorState, PendingTable] = graph match {
        case s @ SplitParam(_, index) => {
          val (key, _) = splits(s.parent)
          
          val source = trans.DerefObjectStatic(Leaf(Source), JPathField(index.toString))
          val spec = buildConstantWrapSpec(source)
          
          state(PendingTable(M.point(key transform spec), graph, TransSpec1.Id))
        }
        
        case s @ SplitGroup(_, index, _) => {
          val (_, map) = splits(s.parent)
          state(PendingTable(M.point(map(index)), graph, TransSpec1.Id))
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
          
          state(PendingTable(M.point(table.get.transform(spec)), graph, TransSpec1.Id))
        }
        
        // TODO technically, we can do this without forcing by pre-lifting PendingTable#trans
        case dag.New(_, parent) => {
          for {
            pendingTable <- loop(parent, splits)
            spec = TableTransSpec.makeTransSpec(
              Map(constants.Key -> trans.WrapArray(Scan(DerefArrayStatic(Leaf(Source), JPathIndex(0)), freshIdScanner))))
            
            tableM2 = for {
              table <- pendingTable.table
              transformed = table.transform(liftToValues(pendingTable.trans))
            } yield table.transform(spec)
          } yield PendingTable(tableM2, graph, TransSpec1.Id)
        }
        
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
          state(PendingTable(M.point(ops.empty), graph, TransSpec1.Id))     // TODO
        
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
            result = pendingTable.table flatMap { parentTable => red(parentTable.transform(DerefObjectStatic(liftedTrans, constants.Value))) }
            wrapped = result map { _ transform buildConstantWrapSpec(Leaf(Source)) }
          } yield PendingTable(wrapped, graph, TransSpec1.Id)
        }

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
                
                back.eval(state): M[Table]
              }
            } yield result
          } 
          table map { PendingTable(_, graph, TransSpec1.Id) }
        }
        
        // VUnion and VIntersect removed, TODO: remove from bytecode
        
        case IUI(_, union, left, right) => {
          for {
            leftPending <- loop(left, splits)
            rightPending <- loop(right, splits)
          } yield {
            val result = for {
              leftPendingTable <- leftPending.table
              rightPendingTable <- rightPending.table
            } yield {
              val leftTable = leftPendingTable.transform(leftPending.trans)
              val rightTable = rightPendingTable.transform(rightPending.trans)
              
              val leftSorted = leftTable.sort(TransSpec1.Id, SortAscending)
              val rightSorted = rightTable.sort(TransSpec1.Id, SortAscending)
              
              val keyValueSpec = trans.ObjectConcat(
                trans.WrapObject(
                  DerefObjectStatic(Leaf(Source), constants.Key),
                  constants.Key.name),
                trans.WrapObject(
                  DerefObjectStatic(Leaf(Source), constants.Value),
                  constants.Value.name))
              
              if (union) {
                leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(Leaf(Source), Leaf(Source), Leaf(SourceLeft))
              } else {
                val emptySpec = trans.Map1(Leaf(Source), ConstantEmptyArray)
                val fullSpec = trans.WrapArray(Leaf(SourceLeft))
              
                val wrapped = leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(emptySpec, emptySpec, fullSpec)
                
                wrapped.transform(DerefArrayStatic(Leaf(Source), JPathIndex(0)))
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
            } yield {
              val leftTable = leftPendingTable.transform(leftPending.trans)
              val rightTable = rightPendingTable.transform(rightPending.trans)
              
              val leftSorted = leftTable.sort(TransSpec1.Id, SortAscending)
              val rightSorted = rightTable.sort(TransSpec1.Id, SortAscending)
              
              val keyValueSpec = trans.ObjectConcat(
                trans.WrapObject(
                  DerefObjectStatic(Leaf(Source), constants.Key),
                  constants.Key.name),
                trans.WrapObject(
                  DerefObjectStatic(Leaf(Source), constants.Value),
                  constants.Value.name))
              
              val emptySpec1 = trans.Map1(Leaf(Source), ConstantEmptyArray)
              val emptySpec2 = trans.Map1(Leaf(SourceLeft), ConstantEmptyArray)
              val fullSpec = trans.WrapArray(Leaf(Source))
              
              val wrappedResult = leftSorted.cogroup(keyValueSpec, keyValueSpec, rightSorted)(fullSpec, emptySpec1, emptySpec2)
              
              wrappedResult.transform(DerefArrayStatic(Leaf(Source), JPathIndex(0)))
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
              state(PendingTable(M.point(ops.empty), graph, TransSpec1.Id))
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
              state(PendingTable(M.point(ops.empty), graph, TransSpec1.Id))
          }
        }
        
        case Join(_, DerefArray, CrossLeftSort | CrossRightSort, left, right) if right.value.isDefined => {
          right.value match {
            case Some(value @ SDecimal(d)) => {
              for {
                pendingTable <- loop(left, splits)
              } yield PendingTable(pendingTable.table, pendingTable.graph, DerefArrayStatic(pendingTable.trans, JPathIndex(d.toInt)))
            }
            
            // TODO other numeric types
            
            case _ =>
              state(PendingTable(M.point(ops.empty), graph, TransSpec1.Id))
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
              state(PendingTable(M.point(ops.empty), graph, TransSpec1.Id))
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
              val key = joinSort match {
                case IdentitySort =>
                  trans.DerefObjectStatic(Leaf(Source), constants.Key)
                
                case ValueSort(id) =>
                  trans.DerefObjectStatic(Leaf(Source), JPathField("sort-" + id))
                
                case _ => sys.error("unreachable code")
              }
              
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
  
        case j @ Join(_, op, joinSort @ (CrossLeftSort | CrossRightSort), left, right) => {
          val isLeft = joinSort == CrossLeftSort
          
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
        
        case dag.Filter(_, joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => {
          // TODO binary typing

          for {
            pendingTableTarget <- loop(target, splits)
            pendingTableBoolean <- loop(boolean, splits)
          } yield {
            if (pendingTableTarget.graph == pendingTableBoolean.graph) {
              PendingTable(pendingTableTarget.table, pendingTableTarget.graph, trans.Filter(pendingTableTarget.trans, pendingTableBoolean.trans))
            }
            else {
              val key = joinSort match {
                case IdentitySort =>
                  trans.DerefObjectStatic(Leaf(Source), constants.Key)
                
                case ValueSort(id) =>
                  trans.DerefObjectStatic(Leaf(Source), JPathField("sort-" + id))
                
                case _ => sys.error("unreachable code")
              }
              
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
        
        case s @ Sort(parent, indexes) => {
          if (indexes == Vector(0 until indexes.length: _*) && parent.sorting == IdentitySort) {
            loop(parent, splits)
          } else {
            val fullOrder = indexes ++ ((0 until parent.provenance.length) filterNot (indexes contains))
            val idSpec = buildIdShuffleSpec(fullOrder)
            
            for {
              pending <- loop(parent, splits)
            } yield {
              val sortedResult = for {
                pendingTable <- pending.table
              } yield {
                val table = pendingTable.transform(liftToValues(pending.trans))
                val shuffled = table.transform(TableTransSpec.makeTransSpec(Map(constants.Key -> idSpec)))
                
                // TODO this could be made more efficient by only considering the indexes we care about
                val sorted = shuffled.sort(DerefObjectStatic(Leaf(Source), constants.Key), SortAscending)
              
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
        
        case SortBy(parent, sortField, valueField, id) => {
          if (parent.sorting == ValueSort(id)) {
            loop(parent, splits)
          } else {
            for {
              pending <- loop(parent, splits)
            } yield {
              val result = for {
                pendingTable <- pending.table
              } yield {
                val table = pendingTable.transform(liftToValues(pending.trans))
                val sorted = table.sort(liftToValues(DerefObjectStatic(Leaf(Source), JPathField(sortField))), SortAscending)
                
                val sortSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), constants.Value), JPathField(sortField))
                val valueSpec = DerefObjectStatic(DerefObjectStatic(Leaf(Source), constants.Value), JPathField(sortField))
                
                val wrappedSort = trans.WrapObject(sortSpec, "sort-" + id)
                val wrappedValue = trans.WrapObject(valueSpec, constants.Value.name)
                
                val oldSortField = parent.sorting match {
                  case ValueSort(id2) if id != id2 =>
                    Some(JPathField("sort-" + id2))
                  
                  case _ => None
                }
                
                val spec = ObjectConcat(
                  ObjectConcat(
                    ObjectDelete(Leaf(Source), Set(JPathField("sort-" + id), constants.Value) ++ oldSortField),
                      wrappedSort),
                      wrappedValue)
                
                sorted.transform(spec)
              }
              
              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case ReSortBy(parent, id) => {
          if (parent.sorting == ValueSort(id)) {
            loop(parent, splits)
          } else {
            for {
              pending <- loop(parent, splits)
            } yield {
              val result = for {
                pendingTable <- pending.table
              } yield {
                val table = pendingTable.transform(liftToValues(pending.trans))
                table.sort(DerefObjectStatic(Leaf(Source), JPathField("sort-" + id)), SortAscending)
              }
              
              PendingTable(result, graph, TransSpec1.Id)
            }
          }
        }
        
        case m @ Memoize(parent, _) =>
          loop(parent, splits)     // TODO
      }
      
      assumptionCheck flatMap { assumedResult: Option[M[Table]] =>
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

    val resultState: StateT[Id, EvaluatorState, M[Table]] = 
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
  
  private def buildIdShuffleSpec(indexes: Vector[Int]): TransSpec1 = {
    indexes map { idx =>
      trans.WrapArray(DerefArrayStatic(Leaf(Source), JPathIndex(idx))): TransSpec1
    } reduce { trans.ArrayConcat(_, _) }
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
      case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(deepMap(source)(f), value, invert)
    }
  }

  sealed trait Context
  
  private case class EvaluatorState(assume: Map[DepGraph, M[Table]] = Map(), extraCount: Int = 0)
  
  private case class PendingTable(table: M[Table], graph: DepGraph, trans: TransSpec1)
}
