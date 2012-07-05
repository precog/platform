package com.precog
package daze

import blueeyes.json.JPath

import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.util._
import com.precog.common.{Path, VectorCase}

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._

import akka.dispatch.{Await, Future}
import akka.util.duration._
import blueeyes.json.{JPathField, JPathIndex}

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.partialFunction._

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
    with TableModule        // TODO specific implementation
    with ImplLibrary
    with InfixLib
    with BigDecimalOperations
    with YggConfigComponent 
    with Logging { self =>
  
  import Function._
  
  import instructions._
  import dag._
  import trans._

  type YggConfig <: EvaluatorConfig 

  sealed trait Context

  implicit def asyncContext: akka.dispatch.ExecutionContext

  def withContext[A](f: Context => A): A = 
    f(new Context {})

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def PrimitiveEqualsF2: F2
  
  def eval(userUID: String, graph: DepGraph, ctx: Context): Table = {
    logger.debug("Eval for %s = %s".format(userUID, graph))

    def loop(graph: DepGraph, assume: Map[DepGraph, Table], splits: Unit, ctx: Context): PendingTable = graph match {
      case g if assume contains g => PendingTable(assume(g), graph, TransSpec1.Id)
      
      case s @ SplitParam(_, index) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case s @ SplitGroup(_, index, _) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case Root(_, instr) => {
        val table = graph.value collect {
          case SString(str) => ops.constString(str)
          case SDecimal(d) => ops.constDecimal(d)
          case SBoolean(b) => ops.constBoolean(b)
          case SNull => ops.constNull
          case SObject(map) if map.isEmpty => ops.constEmptyObject
          case SArray(Vector()) => ops.constEmptyArray
        }
        
        PendingTable(table.get, graph, TransSpec1.Id)       // die a horrible death if isEmpty
      }
      
      case dag.New(_, parent) => loop(parent, assume, splits, ctx)   // TODO John swears this part is easy
      
      case dag.LoadLocal(_, _, parent, _) => {
        val back = parent.value match {
          case Some(SString(str)) => ops.loadStatic(str)
          case Some(_) => ops.empty
          
          case None => {
            val PendingTable(table, _, trans) = loop(parent, assume, splits, ctx)
            ops.loadDynamic(table.transform(liftToValues(trans)))
          }
        }
        
        PendingTable(back, graph, TransSpec1.Id)
      }

      case dag.SetReduce(_, Distinct, parent) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case Operate(_, instructions.WrapArray, parent) => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(parent, assume, splits, ctx)
        PendingTable(parentTable, parentGraph, trans.WrapArray(parentTrans))
      }
      
      case o @ Operate(_, op, parent) => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(parent, assume, splits, ctx)
        
        // TODO unary typing
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, op.f1))
      }
      
      case r @ dag.Reduce(_, red, parent) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case s @ dag.Split(line, specs, child) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      // VUnion and VIntersect removed, TODO: remove from bytecode
      
      case Join(_, instr @ (IUnion | IIntersect | SetDifference), left, right) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case Join(_, Map2Cross(Eq) | Map2CrossLeft(Eq) | Map2CrossRight(Eq), left, right) if right.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, equalsF2.partialRight(svalueToCValue(right.value.get))))
      }
      
      case Join(_, Map2Cross(Eq) | Map2CrossLeft(Eq) | Map2CrossRight(Eq), left, right) if left.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(right, assume, splits, ctx)
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, equalsF2.partialLeft(svalueToCValue(left.value.get))))
      }
      
      case Join(_, Map2Cross(NotEq) | Map2CrossLeft(NotEq) | Map2CrossRight(NotEq), left, right) if right.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
        val eqTrans = trans.Map1(parentTrans, PrimitiveEqualsF2.partialRight(svalueToCValue(right.value.get)))
        PendingTable(parentTable, parentGraph, trans.Map1(eqTrans, Comp.f1))   // TODO
      }
      
      case Join(_, Map2Cross(NotEq) | Map2CrossLeft(NotEq) | Map2CrossRight(NotEq), left, right) if left.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(right, assume, splits, ctx)
        val eqTrans = trans.Map1(parentTrans, PrimitiveEqualsF2.partialRight(svalueToCValue(left.value.get)))
        PendingTable(parentTable, parentGraph, trans.Map1(eqTrans, Comp.f1))   // TODO
      }
      
      case Join(_, Map2Cross(WrapObject) | Map2CrossLeft(WrapObject) | Map2CrossRight(WrapObject), left, right) if left.value.isDefined => {
        left.value match {
          case Some(value @ SString(str)) => {
            val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
            PendingTable(parentTable, parentGraph, WrapStatic(parentTrans, str))
          }
          
          case _ =>
            PendingTable(ops.empty, graph, TransSpec1.Id)
        }
      }
      
      case Join(_, Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), left, right) if right.value.isDefined => {
        right.value match {
          case Some(value @ SString(str)) => {
            val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
            PendingTable(parentTable, parentGraph, DerefObjectStatic(parentTrans, JPath(str)))
          }
          
          case _ =>
            PendingTable(ops.empty, graph, TransSpec1.Id)
        }
      }
      
      case Join(_, Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), left, right) if right.value.isDefined => {
        right.value match {
          case Some(value @ SDecimal(d)) => {     // TODO other numeric types
            val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
            PendingTable(parentTable, parentGraph, DerefArrayStatic(parentTrans, d.toInt))
          }
          
          case _ =>
            PendingTable(ops.empty, graph, TransSpec1.Id)
        }
      }
      
      case Join(_, Map2Cross(ArraySwap) | Map2CrossLeft(ArraySwap) | Map2CrossRight(ArraySwap), left, right) if right.value.isDefined => {
        right.value match {
          case Some(value @ SDecimal(d)) => {     // TODO other numeric types
            val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
            PendingTable(parentTable, parentGraph, ArraySwap(parentTrans, d.toLong))
          }
          
          case _ =>
            PendingTable(ops.empty, graph, TransSpec1.Id)
        }
      }

      // case Join(_, Map2CrossLeft(op), left, right) if right.isSingleton =>
      
      // case Join(_, Map2CrossRight(op), left, right) if left.isSingleton =>
      
      // begin: annoyance with Scala's lousy pattern matcher
      case Join(_, Map2Cross(_) | Map2CrossRight(_) | Map2CrossLeft(_), left, right) if right.value.isDefined => {
        val op = graph match {
          case Map2Cross(op) => op
          case Map2CrossRight(op) => op
          case Map2CrossLeft(op) => op
        }
        
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
        
        val cv = svalueToCValue(right.value.get)
        val f1 = op.f2.partialRight(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }
      
      case Join(_, Map2Cross(_) | Map2CrossLeft(_), left, right) if left.value.isDefined => {
        val op = graph match {
          case Map2Cross(op) => op
          case Map2CrossRight(op) => op
          case Map2CrossLeft(op) => op
        }
        
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(right, assume, splits, ctx)
        
        val cv = svalueToCValue(left.value.get)
        val f1 = op.f2.partialLeft(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }
      // end: annoyance
      
      case Join(_, Map2Match(op), left, right) => {
        lazy val length = sharedPrefixLength(left, right)
        
        // TODO binary typing
        
        val PendingTable(parentLeftTable, parentLeftGraph, parentLeftTrans) = loop(left, assume, splits, ctx)
        val PendingTable(parentRightTable, parentRightGraph, parentRightTrans) = loop(right, assume, splits, ctx)
        
        if (parentLeftGraph == parentRightGraph) {
          val tf: (TransSpec1, TransSpec1) => TransSpec1 = op match {
            case Eq => trans.Equal
            case NotEq => { (left, right) => trans.Map1(trans.Equal(left, right), Comp) }     // TODo
            case WrapObject => WrapDynamic
            case JoinObject => ObjectConcat
            case JoinArray => ArrayConcat
            case instructions.ArraySwap => sys.error("nothing happens")
            case DerefObject => DerefObjectDynamic
            case DerefArray => DerefArrayDynamic
            case _ => { (left, right) => trans.Map2(left, right, op.f2) }
          }
          
          PendingTable(parentLeftTable, parentLeftGraph, tf(parentLeftTrans, parentRightTrans))
        } else {
          PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
        } 
      }

      // guaranteed: cross, cross_left and cross_right
      case j @ Join(_, instr, left, right) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case dag.Filter(_, None, _, target, boolean) => {
        lazy val length = sharedPrefixLength(target, boolean)
        
        // TODO binary typing
        
        val PendingTable(parentTargetTable, parentTargetGraph, parentTargetTrans) = loop(target, assume, splits, ctx)
        val PendingTable(parentBooleanTable, parentBooleanGraph, parentBooleanTrans) = loop(boolean, assume, splits, ctx)
        
        if (parentTargetGraph == parentBooleanGraph)
          PendingTable(parentTargetTable, parentTargetGraph, trans.Filter(parentTargetTrans, parentBooleanTrans))
        else
          PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      }
      
      case f @ dag.Filter(_, Some(cross), _, target, boolean) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case s @ Sort(parent, indexes) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case m @ Memoize(parent, _) =>
        loop(parent, assume, splits, ctx)     // TODO
    }
    
    val PendingTable(table, _, spec) = loop(memoize(orderCrosses(graph)), Map(), (), ctx)
    table.transform(liftToValues(spec))
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
  
  private def liftToValues(trans: TransSpec1): TransSpec1 =
    TableTransSpec.makeTransSpec(Map(TableKVConstants.Value -> trans))
  
  
  private case class PendingTable(table: Table, graph: DepGraph, trans: TransSpec1)
}
