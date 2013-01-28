package com.precog
package daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.CLong

import scalaz.std.map._

trait EvaluatorMethodsModule[M[+_]] extends DAG with OpFinderModule[M] {
  import library._
  import trans._

  trait EvaluatorMethods extends OpFinder {
    import dag._ 
    import instructions._

    type TableTransSpec[+A <: SourceType] = Map[CPathField, TransSpec[A]]
    type TableTransSpec1 = TableTransSpec[Source1]
    type TableTransSpec2 = TableTransSpec[Source2]
    
    def transFromBinOp[A <: SourceType](op: BinaryOperation, ctx: EvaluationContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
      case Eq => trans.Equal[A](left, right)
      case NotEq => op1ForUnOp(Comp).spec(ctx)(trans.Equal[A](left, right))
      case instructions.WrapObject => WrapObjectDynamic(left, right)
      case JoinObject => InnerObjectConcat(left, right)
      case JoinArray => InnerArrayConcat(left, right)
      case instructions.ArraySwap => sys.error("nothing happens")
      case DerefObject => DerefObjectDynamic(left, right)
      case DerefMetadata => sys.error("cannot do a dynamic metadata deref")
      case DerefArray => DerefArrayDynamic(left, right)
      case _ => trans.Map2(left, right, op2ForBinOp(op).get.f2(ctx))     // if this fails, we're missing a case above
    }

    def makeTableTrans(tableTrans: TableTransSpec1): TransSpec1 = {
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
}

trait ReductionFinderModule[M[+_]] extends DAG with EvaluatorMethodsModule[M] {
  type TS1 = trans.TransSpec1
  import library._
  import trans._

  trait ReductionFinder extends EvaluatorMethods {
    import dag._ 
    import instructions._

    case class ReduceInfo(reduce: dag.Reduce, spec: TransSpec1, ancestor: DepGraph)

    // for a reduce, build the single transpecable chain, ignoring other irrelevant branches
    def buildReduceInfo(reduce: dag.Reduce, ctx: EvaluationContext): ReduceInfo = {
      def loop(graph: DepGraph, f: TransSpec1 => TransSpec1): (TransSpec1, DepGraph) = graph match {
        case Join(Eq, _, left, Const(value)) =>
          loop(left, t => f(trans.EqualLiteral(t, value, false)))

        case Join(Eq, _, Const(value), right) =>
          loop(right, t => f(trans.EqualLiteral(t, value, false)))

        case Join(NotEq, _, left, Const(value)) =>
          loop(left, t => f(trans.EqualLiteral(t, value, true)))

        case Join(NotEq, _, Const(value), right) =>
          loop(right, t => f(trans.EqualLiteral(t, value, true)))

        case Join(instructions.WrapObject, _, Const(value), right) =>
          value match {
            case value @ CString(str) => loop(right, t => f(trans.WrapObject(t, str)))
            case _ => (f(Leaf(Source)), graph)
          }

        case Join(instructions.DerefObject, _, left, Const(value)) =>
          value match {
            case value @ CString(str) => loop(left, t => f(DerefObjectStatic(t, CPathField(str))))
            case _ => (f(Leaf(Source)), graph)
          }
        
        case Join(instructions.DerefMetadata, _, left, Const(value)) =>
          value match {
            case value @ CString(str) => loop(left, t => f(DerefMetadataStatic(t, CPathMeta(str))))
            case _ => (f(Leaf(Source)), graph)
          }

        case Join(DerefArray, _, left, Const(value)) =>
          value match {
            case CNum(n) => loop(left, t => f(DerefArrayStatic(t, CPathIndex(n.toInt))))
            case CLong(n) => loop(left, t => f(DerefArrayStatic(t, CPathIndex(n.toInt))))
            case CDouble(n) => loop(left, t => f(DerefArrayStatic(t, CPathIndex(n.toInt))))
            case _ => (f(Leaf(Source)), graph)
          }
        
        case Join(instructions.ArraySwap, _, left, Const(value)) =>
          value match {
            case CNum(n) => loop(left, t => f(trans.ArraySwap(t, n.toInt)))
            case CLong(n) => loop(left, t => f(trans.ArraySwap(t, n.toInt)))
            case CDouble(n) => loop(left, t => f(trans.ArraySwap(t, n.toInt)))
            case _ => (f(Leaf(Source)), graph)
          }

        case Join(instructions.JoinObject, _, left, Const(value)) =>
          value match {
            case CEmptyObject => loop(left, t => f(trans.InnerObjectConcat(t)))
            case _ => (f(Leaf(Source)), graph)
          }
          
        case Join(instructions.JoinObject, _, Const(value), right) =>
          value match {
            case CEmptyObject => loop(right, t => f(trans.InnerObjectConcat(t)))
            case _ => (f(Leaf(Source)), graph)
          }

        case Join(instructions.JoinArray, _, left, Const(value)) =>
          value match {
            case CEmptyArray => loop(left, t => f(trans.InnerArrayConcat(t)))
            case _ => (f(Leaf(Source)), graph)
          }

        case Join(instructions.JoinArray, _, Const(value), right) =>
          value match {
            case CEmptyArray => loop(right, t => f(trans.InnerArrayConcat(t)))
            case _ => (f(Leaf(Source)), graph)
          }

        case Join(op, _, left, Const(value)) =>
          op2ForBinOp(op) map { _.f2(ctx).applyr(value) } match {
            case Some(f1) => loop(left, t => f(trans.Map1(t, f1)))
            case None => (f(Leaf(Source)), graph)
          }
            
        case Join(op, CrossLeftSort | CrossRightSort, Const(value), right) =>
          op2ForBinOp(op) map { _.f2(ctx).applyl(value) } match {
            case Some(f1) => loop(right, t => f(trans.Map1(t, f1)))
            case None => (f(Leaf(Source)), graph)
          }

        case dag.Join(op, joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => 
          val (targetTrans, targetAncestor) = loop(target, identity _)
          val (booleanTrans, booleanAncestor) = loop(boolean, identity _)

          if (targetAncestor == booleanAncestor) (f(transFromBinOp(op, ctx)(targetTrans, booleanTrans)), targetAncestor)
          else (f(Leaf(Source)), graph)

        case dag.Filter(joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => 
          val (targetTrans, targetAncestor) = loop(target, identity _)
          val (booleanTrans, booleanAncestor) = loop(boolean, identity _)

          if (targetAncestor == booleanAncestor) (f(trans.Filter(targetTrans, booleanTrans)), targetAncestor)
          else (f(Leaf(Source)), graph)

        case dag.Operate(instructions.WrapArray, parent) => loop(parent, t => f(trans.WrapArray(t)))

        case dag.Operate(op, parent) => loop(parent, t => f(op1ForUnOp(op).spec(ctx)(t)))

        case _ => (f(Leaf(Source)), graph)
      }

      val (spec, ancestor) = loop(reduce.parent, identity _)
      ReduceInfo(reduce, spec, ancestor)
    }

    def findReductions(node: DepGraph, ctx: EvaluationContext): MegaReduceState = {
      implicit val m = new Monoid[List[dag.Reduce]] {
        def zero: List[dag.Reduce] = Nil
        def append(x: List[dag.Reduce], y: => List[dag.Reduce]) = x ::: y
      }

      val reduces = node.foldDown[List[dag.Reduce]](true) {
        case r: dag.Reduce => List(r)
      } distinct

      val info: List[ReduceInfo] = reduces map { buildReduceInfo(_: dag.Reduce, ctx) }

      // for each reduce node, associate it with its ancestor
      val (ancestorByReduce, specByParent) = info.foldLeft((Map[dag.Reduce, DepGraph](), Map[DepGraph, TransSpec1]())) {
        case ((ancestorByReduce, specByParent), ReduceInfo(reduce, spec, ancestor)) =>
          (ancestorByReduce + (reduce -> ancestor), specByParent + (reduce.parent -> spec))
      }

      // for each ancestor, assemble a list of the parents it created
      val parentsByAncestor = (info groupBy { _.ancestor }).foldLeft(Map[DepGraph, List[DepGraph]]()) {
        case (parentsByAncestor, (ancestor, lst)) =>
          parentsByAncestor + (ancestor -> (lst map { _.reduce.parent } distinct))
      }

      // for each parent, assemble a list of the reduces it created
      val reducesByParent = (info groupBy { _.reduce.parent }).foldLeft(Map[DepGraph, List[dag.Reduce]]()) {
        case (reducesByParent, (parent, lst)) =>
          reducesByParent + (parent -> (lst map { _.reduce }))
      }

      MegaReduceState(ancestorByReduce, parentsByAncestor, reducesByParent, specByParent)
    }

    case class MegaReduceState(
        ancestorByReduce: Map[dag.Reduce, DepGraph],
        parentsByAncestor: Map[DepGraph, List[DepGraph]],
        reducesByParent: Map[DepGraph, List[dag.Reduce]],
        specByParent: Map[DepGraph, TransSpec1]) {
          
      def buildMembers(ancestor: DepGraph): List[(TransSpec1, List[Reduction])] = {
        parentsByAncestor(ancestor) map {
          p => (specByParent(p), reducesByParent(p) map { _.red })
        }
      }
    }

    def megaReduce(node: DepGraph, st: MegaReduceState): DepGraph = {
      val reduceTable = mutable.Map[DepGraph, dag.MegaReduce]() 

      node mapDown { recurse => {
        case graph @ dag.Reduce(red, parent) if st.ancestorByReduce contains graph => {
          val ancestor = st.ancestorByReduce(graph)
          val members = st.buildMembers(ancestor)

          val left = reduceTable get ancestor getOrElse {
            val result = dag.MegaReduce(members, recurse(ancestor))
            reduceTable(ancestor) = result
            result
          }

          val firstIndex = st.parentsByAncestor(ancestor).reverse indexOf parent
          val secondIndex = st.reducesByParent(parent).reverse indexOf graph

          dag.Join(DerefArray, CrossLeftSort, 
            dag.Join(DerefArray, CrossLeftSort, 
              left,
              Const(CLong(firstIndex))(graph.loc))(graph.loc),
            Const(CLong(secondIndex))(graph.loc))(graph.loc)
        }
      }}
    }
  }
}
