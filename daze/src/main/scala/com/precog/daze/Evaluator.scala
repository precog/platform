package com.precog
package daze

import memoization._
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
    with MatchAlgebra
    with OperationsAPI
    with MemoizationEnvironment
    with ImplLibrary
    with InfixLib
    with BigDecimalOperations
    with YggConfigComponent 
    with Logging { self =>
  
  import Function._
  
  import instructions._
  import dag._
  import trans._

  type Dataset[E]
  type Memoable[E]
  type Grouping[K, A]
  type YggConfig <: EvaluatorConfig 

  sealed trait Context {
    def memoizationContext: MemoContext
    def expiration: Long
    def nextId(): Long
    def release: Release
  }

  implicit def asyncContext: akka.dispatch.ExecutionContext

  implicit def extend[E](d: Dataset[E]): DatasetExtensions[Dataset, Memoable, Grouping, E] = ops.extend(d)

  def withContext[A](f: Context => A): A = {
    withMemoizationContext { memoContext => 
      val ctx = new Context { 
        val memoizationContext = memoContext
        val expiration = System.currentTimeMillis + yggConfig.maxEvalDuration.toMillis 
        def nextId() = yggConfig.idSource.nextId()
        val release = new Release(IO())
      }

      f(ctx)
    }
  }

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def eval(userUID: String, graph: DepGraph, ctx: Context): Table = {
    logger.debug("Eval for %s = %s".format(userUID, graph))

    def findCommonality(seen: Set[DepGraph])(graphs: DepGraph*): Option[DepGraph] = {
      val (seen2, next, back) = graphs.foldLeft((seen, Vector[DepGraph](), None: Option[DepGraph])) {
        case (pair @ (_, _, Some(_)), _) => pair
        
        case ((seen, _, _), graph) if seen(graph) => (seen, Vector(), Some(graph))
        
        case ((oldSeen, next, None), graph) => {
          val seen = oldSeen + graph
          
          graph match {
            case SplitParam(_, _) | SplitGroup(_, _, _) | Root(_, _) | dag.Split(_, _, _) =>
              (seen, next, None)
              
            case dag.New(_, parent) =>
              (seen, next :+ parent, None)
            
            case dag.SetReduce(_, _, parent) =>
              (seen, next :+ parent, None)
            
            case dag.LoadLocal(_, _, parent, _) =>
              (seen, next :+ parent, None)
            
            case Operate(_, _, parent) =>
              (seen, next :+ parent, None)
            
            case dag.Reduce(_, _, parent) =>
              (seen, next :+ parent, None)
            
            case Join(_, _, left, right) =>
              (seen, next :+ left :+ right, None)
            
            case Filter(_, _, _, target, boolean) =>
              (seen, next :+ target :+ boolean, None)
            
            case Sort(parent, _) =>
              (seen, next :+ parent, None)
            
            case Memoize(parent, _) =>
              (seen, next :+ parent, None)
          }
        }
      }
      
      if (back.isDefined || next.isEmpty)
        back
      else
        findCommonality(seen2)(next: _*)
    }
  
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
          case SObject(Map()) => ops.constEmptyObject
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
            val PendingSpec(table, _, trans) = loop(parent, assume, splits, ctx)
            ops.loadDynamic(table.transform(trans))
          }
        }
        
        PendingTable(back, graph, TransSpec1.Id)
      }

      case dag.SetReduce(_, Distinct, parent) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case o @ Operate(_, op, parent) => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(parent, assume, splits, ctx)
        
        // TODO unary typing
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, op.f1))
      }
      
      case r @ dag.Reduce(_, red, parent) =>
        PendingTable(ops.empty, graph, trans.Id)     // TODO
      
      case s @ dag.Split(line, specs, child) =>
        PendingTable(ops.empty, graph, trans.Id)     // TODO
      
      // VUnion and VIntersect removed, TODO: remove from bytecode
      
      case Join(_, instr @ (IUnion | IIntersect | SetDifference), left, right) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
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

      case Join(_, Map2Cross(op @ BuiltInFunction2Op(f)), left, right) if f.requiresReduction =>
        PendingTable(ops.empty, graph, TransSpec1.Id)      // TODO

      case Join(_, Map2CrossLeft(op @ BuiltInFunction2Op(f)), left, right) if f.requiresReduction =>
        PendingTable(ops.empty, graph, TransSpec1.Id)      // TODO

      case Join(_, Map2CrossRight(op @ BuiltInFunction2Op(f)), left, right) if f.requiresReduction =>
        PendingTable(ops.empty, graph, TransSpec1.Id)      // TODO

      // case Join(_, Map2CrossLeft(op), left, right) if right.isSingleton =>
      
      // case Join(_, Map2CrossRight(op), left, right) if left.isSingleton =>
      
      // begin: annoyance with Scala's lousy pattern matcher
      case Join(_, Map2Cross(op), left, right) if right.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
        
        val cv = svalueToCValue(right.value.get)
        val f1 = op.f2.partialRight(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }
      
      case Join(_, Map2CrossRight(op), left, right) if right.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
        
        val cv = svalueToCValue(right.value.get)
        val f1 = op.f2.partialRight(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }      

      case Join(_, Map2CrossLeft(op), left, right) if right.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(left, assume, splits, ctx)
        
        val cv = svalueToCValue(right.value.get)
        val f1 = op.f2.partialRight(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }
      
      case Join(_, Map2Cross(op), left, right) if left.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(right, assume, splits, ctx)
        
        val cv = svalueToCValue(left.value.get)
        val f1 = op.f2.partialLeft(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }

      case Join(_, Map2CrossRight(op), left, right) if left.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(right, assume, splits, ctx)
        
        val cv = svalueToCValue(left.value.get)
        val f1 = op.f2.partialLeft(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }

      case Join(_, Map2CrossLeft(op), left, right) if left.value.isDefined => {
        val PendingTable(parentTable, parentGraph, parentTrans) = loop(right, assume, splits, ctx)
        
        val cv = svalueToCValue(left.value.get)
        val f1 = op.f2.partialLeft(cv)
        
        PendingTable(parentTable, parentGraph, trans.Map1(parentTrans, f1))
      }
      // end: annoyance
      
      case Join(_, Map2Match(op), left, right) => {
        lazy val length = sharedPrefixLength(left, right)
        val f2 = op.f2
        
        // TODO binary typing
        
        val PendingTable(parentLeftTable, parentLeftGraph, parentLeftTrans) = loop(left, assume, splits, ctx)
        val PendingTable(parentRightTable, parentRightGraph, parentRightTrans) = loop(right, assume, splits, ctx)
        
        op match {
          case BuiltInFunction2Op(f) if f.requiresReduction =>
            PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
          
          case _ => { 
            if (parentLeftGraph == parentRightGraph) 
              PendingTable(parentLeftTable, parentLeft, trans.Map2(parentLeftTrans, parentRightTrans, f2))  
            else 
              PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
          }
        }
      }

      // guaranteed: cross, cross_left and cross_right
      case j @ Join(_, instr, left, right) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case Filter(_, None, _, target, boolean) => {
        lazy val length = sharedPrefixLength(target, boolean)
        
        // TODO binary typing
        
        val PendingTable(parentTargetTable, parentTargetGraph, parentTargetTrans) = loop(target, assume, splits, ctx)
        val PendingTable(parentBooleanTable, parentBooleanGraph, parentBooleanTrans) = loop(boolean, assume, splits, ctx)
        
        if (parentTargetGraph == parentBooleanGraph)
          PendingTable(parentTargetTable, parentTargetGraph, trans.Filter(parentTargetTrans, parentBooleanTrans))
        else
          PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      }
      
      case f @ Filter(_, Some(cross), _, target, boolean) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case s @ Sort(parent, indexes) =>
        PendingTable(ops.empty, graph, TransSpec1.Id)     // TODO
      
      case m @ Memoize(parent, _) =>
        loop(parent, assume, splits, ctx)     // TODO
    }
    
    val PendingTable(table, _, trans) = loop(memoize(orderCrosses(graph)), Map(), Map(), ctx)
    table.transform(trans)
  }
  
  private def binaryOp(op: BinaryOperation): BIF2 = {
    op match {
      case Add => Infix.Add
      case Sub => Infix.Sub
      case Mul => Infix.Mul
      case Div => Infix.Div
      
      case Lt   => Infix.Lt
      case LtEq => Infix.LtEq
      case Gt   => Infix.Gt
      case GtEq => Infix.GtEq
      
      case Eq    => Infix.Eq
      case NotEq => Infix.NotEq
      
      case And => Infix.And
      case Or  => Infix.Or
      
      case WrapObject => Infix.WrapObject
      
      case JoinObject => Infix.JoinObject
      case JoinArray  => Infix.JoinArray
      
      case ArraySwap  => Infix.ArraySwap
      
      case DerefObject => Infix.DerefObject
      case DerefArray  => Infix.DerefArray

      case BuiltInFunction2Op(f) => f
    }
  }

  private def sharedPrefixLength(left: DepGraph, right: DepGraph): Int =
    left.provenance zip right.provenance takeWhile { case (a, b) => a == b } length
  
  private def svalueToCValue(sv: SValue) = sv match {
    case SString(str) => CString(str)
    case SDecimal(d) => CDecimal(d)
    case SLong(l) => CLong(l)
    case SDouble(d) => CDouble(d)
    case SNull => CNull
    case _ => sys.error("die a horrible death")
  }
  
  
  case class PendingTable(table: Table, graph: DepGraph, trans: TransSpec1)
}
