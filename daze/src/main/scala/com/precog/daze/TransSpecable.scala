package com.precog.daze

import com.precog.common.Path
import com.precog.common.json.{ CPathField, CPathIndex, CPathMeta }
import com.precog.yggdrasil.{ CDouble, CEmptyArray, CEmptyObject, CLong, CNum, CString, CValue } 
import com.precog.yggdrasil.table.CF1

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monadPlus._

trait TransSpecable[M[+_]] extends DAG with EvaluatorMethods[M] {
  import dag._ 
  import instructions._
  import trans.TransSpec1
  
  trait TransSpecableFold[T] {
    def EqualLiteral(node: Join)(parent: T, value: CValue, invert: Boolean): T
    def WrapObject(node: Join)(parent: T, field: String): T
    def DerefObjectStatic(node: Join)(parent: T, field: String): T
    def DerefMetadataStatic(node: Join)(parent: T, field: String): T
    def DerefArrayStatic(node: Join)(parent: T, index: Int): T
    def ArraySwap(node: Join)(parent: T, index: Int): T
    def InnerObjectConcat(node: Join)(parent: T): T
    def InnerArrayConcat(node: Join)(parent: T): T
    def Map1Left(node: Join)(parent: T, op: Op2, graph: DepGraph, value: CValue): T
    def Map1Right(node: Join)(parent: T, op: Op2, graph: DepGraph, value: CValue): T
    def binOp(node: Join)(leftParent: T, rightParent: => T, op: BinaryOperation): T
    def Filter(node: Filter)(leftParent: T, rightParent: => T): T
    def WrapArray(node: Operate)(parent: T): T
    def Map1(node: Operate)(parent: T, op: UnaryOperation): T
    def unmatched(node: DepGraph): T
    def done(node: DepGraph): T
  }
  
  def isTransSpecable(to: DepGraph, from: DepGraph): Boolean = foldDownTransSpecable(to, Some(from))(new TransSpecableFold[Boolean] {
    def EqualLiteral(node: Join)(parent: Boolean, value: CValue, invert: Boolean) = parent
    def WrapObject(node: Join)(parent: Boolean, field: String) = parent
    def DerefObjectStatic(node: Join)(parent: Boolean, field: String) = parent
    def DerefMetadataStatic(node: Join)(parent: Boolean, field: String) = parent
    def DerefArrayStatic(node: Join)(parent: Boolean, index: Int) = parent
    def ArraySwap(node: Join)(parent: Boolean, index: Int) = parent
    def InnerObjectConcat(node: Join)(parent: Boolean) = parent
    def InnerArrayConcat(node: Join)(parent: Boolean) = parent
    def Map1Left(node: Join)(parent: Boolean, op: Op2, graph: DepGraph, value: CValue) = parent
    def Map1Right(node: Join)(parent: Boolean, op: Op2, graph: DepGraph, value: CValue) = parent
    def binOp(node: Join)(leftParent: Boolean, rightParent: => Boolean, op: BinaryOperation) = leftParent && rightParent
    def Filter(node: Filter)(leftParent: Boolean, rightParent: => Boolean) = leftParent && rightParent
    def WrapArray(node: Operate)(parent: Boolean) = parent
    def Map1(node: Operate)(parent: Boolean, op: UnaryOperation) = parent
    def unmatched(node: DepGraph) = false
    def done(node: DepGraph) = true
  })
  
  def snd[A, B](a: A, b: B) = b
  
  def mkTransSpec(to: DepGraph, from: DepGraph, ctx: EvaluationContext) =
    mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, Some(from), ctx, identity, snd, some).map(_._1)  

  def findTransSpecAndAncestor(to: DepGraph, ctx: EvaluationContext) =
    mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, None, ctx, identity, snd, some)  
    
  def mkTransSpecWithState[M[+_] : Monad, S](to: DepGraph, from: Option[DepGraph], ctx: EvaluationContext, get: S => (TransSpec1, DepGraph), set: (S, (TransSpec1, DepGraph)) => S, init: ((TransSpec1, DepGraph)) => M[S]): M[S] = {
    foldDownTransSpecable(to, from)(new TransSpecableFold[M[S]] {
      import trans._

      // Bifunctor leftMap would be better here if it existed in pimped type inferrable form
      def leftMap(parent: S)(f: TransSpec1 => TransSpec1) = get(parent) match { 
        case (spec, ancestor) => set(parent, (f(spec), ancestor))
      }
      
      def EqualLiteral(node: Join)(parent: M[S], value: CValue, invert: Boolean) =
        parent.map(leftMap(_)(trans.EqualLiteral(_, value, invert)))
        
      def WrapObject(node: Join)(parent: M[S], field: String) =
        parent.map(leftMap(_)(trans.WrapObject(_, field)))
        
      def DerefObjectStatic(node: Join)(parent: M[S], field: String) =
        parent.map(leftMap(_)(trans.DerefObjectStatic(_, CPathField(field))))
      
      def DerefMetadataStatic(node: Join)(parent: M[S], field: String) =
        parent.map(leftMap(_)(trans.DerefMetadataStatic(_, CPathMeta(field))))
      
      def DerefArrayStatic(node: Join)(parent: M[S], index: Int) =
        parent.map(leftMap(_)(trans.DerefArrayStatic(_, CPathIndex(index))))
      
      def ArraySwap(node: Join)(parent: M[S], index: Int) = 
        parent.map(leftMap(_)(trans.ArraySwap(_, index)))
      
      def InnerObjectConcat(node: Join)(parent: M[S]) =
        parent.map(leftMap(_)(trans.InnerObjectConcat(_)))
        
      def InnerArrayConcat(node: Join)(parent: M[S]) =
        parent.map(leftMap(_)(trans.InnerArrayConcat(_)))

      def Map1Left(node: Join)(parent: M[S], op: Op2, graph: DepGraph, value: CValue) =
        parent.map(leftMap(_)(trans.Map1(_, op.f2(ctx).partialRight(value))))
        
      def Map1Right(node: Join)(parent: M[S], op: Op2, graph: DepGraph, value: CValue) =
        parent.map(leftMap(_)(trans.Map1(_, op.f2(ctx).partialLeft(value))))
      
      def binOp(node: Join)(leftParent: M[S], rightParent: => M[S], op: BinaryOperation) = {
        for {
          pl      <- leftParent
          (l, al) =  get(pl)
          pr      <- rightParent
          (r, ar) =  get(pr)
          result  <- if(al == ar) set(pl, (transFromBinOp(op, ctx)(l, r), al)).point[M] else init(Leaf(Source), node)  
        } yield result
      }
        
      def Filter(node: dag.Filter)(leftParent: M[S], rightParent: => M[S]) = 
        for {
          pl      <- leftParent
          (l, al) =  get(pl)
          pr      <- rightParent
          (r, ar) =  get(pr)
          result  <- if(al == ar) set(pl, (trans.Filter(l, r), al)).point[M] else init(Leaf(Source), node)
        } yield result
      
      def WrapArray(node: Operate)(parent: M[S]) =
        parent.map(leftMap(_)(trans.WrapArray(_)))
      
      def Map1(node: Operate)(parent: M[S], op: UnaryOperation) =
        parent.map(leftMap(_)(trans.Map1(_, op1(op).f1(ctx))))

      def unmatched(node: DepGraph) = init(Leaf(Source), node)
      
      def done(node: DepGraph) = init(Leaf(Source), node)
    })
  }

  def foldDownTransSpecable[T](to: DepGraph, from: Option[DepGraph])(alg: TransSpecableFold[T]): T = {
    
    object ConstInt {
      def unapply(c: Const) = c match {
        case Const(_, CNum(n)) => Some(n.toInt)
        case Const(_, CLong(n)) => Some(n.toInt)
        case Const(_, CDouble(n)) => Some(n.toInt)
        case _ => None
      }
    }
    
    object Op2ForBinOp {
      def unapply(op: BinaryOperation) = op2ForBinOp(op)
    }

    def loop(graph: DepGraph): T = graph match {
      case node if from.map(_ == node).getOrElse(false) => alg.done(node)
        
      case node @ Join(_, Eq, CrossLeftSort | CrossRightSort, left, Const(_, value)) =>
        alg.EqualLiteral(node)(loop(left), value, false)

      case node @ Join(_, Eq, CrossLeftSort | CrossRightSort, Const(_, value), right) =>
        alg.EqualLiteral(node)(loop(right), value, false)

      case node @ Join(_, NotEq, CrossLeftSort | CrossRightSort, left, Const(_, value)) =>
        alg.EqualLiteral(node)(loop(left), value, true)

      case node @ Join(_, NotEq, CrossLeftSort | CrossRightSort, Const(_, value), right) =>
        alg.EqualLiteral(node)(loop(right), value, true)

      case node @ Join(_, WrapObject, CrossLeftSort | CrossRightSort, Const(_, CString(field)), right) =>
        alg.WrapObject(node)(loop(right), field)

      case node @ Join(_, DerefObject, CrossLeftSort | CrossRightSort, left, Const(_, CString(field))) =>
        alg.DerefObjectStatic(node)(loop(left), field)
      
      case node @ Join(_, DerefMetadata, CrossLeftSort | CrossRightSort, left, Const(_, CString(field))) =>
        alg.DerefMetadataStatic(node)(loop(left), field)

      case node @ Join(_, DerefArray, CrossLeftSort | CrossRightSort, left, ConstInt(index)) =>
        alg.DerefArrayStatic(node)(loop(left), index)
      
      case node @ Join(_, ArraySwap, CrossLeftSort | CrossRightSort, left, ConstInt(index)) =>
        alg.ArraySwap(node)(loop(left), index)

      case node @ Join(_, JoinObject, CrossLeftSort | CrossRightSort, left, Const(_, CEmptyObject)) =>
        alg.InnerObjectConcat(node)(loop(left))
                  
      case node @ Join(_, JoinObject, CrossLeftSort | CrossRightSort, Const(_, CEmptyObject), right) =>
        alg.InnerObjectConcat(node)(loop(right))

      case node @ Join(_, JoinArray, CrossLeftSort | CrossRightSort, left, Const(_, CEmptyArray)) =>
        alg.InnerArrayConcat(node)(loop(left))
                  
      case node @ Join(_, JoinArray, CrossLeftSort | CrossRightSort, Const(_, CEmptyArray), right) =>
        alg.InnerArrayConcat(node)(loop(right))

      case node @ Join(_, Op2ForBinOp(op), CrossLeftSort | CrossRightSort, left, Const(_, value)) =>
        alg.Map1Left(node)(loop(left), op, left, value)

      case node @ Join(_, Op2ForBinOp(op), CrossLeftSort | CrossRightSort, Const(_, value), right) =>
        alg.Map1Right(node)(loop(right), op, right, value) 

      case node @ Join(_, op, joinSort @ (IdentitySort | ValueSort(_)), left, right) => 
        alg.binOp(node)(loop(left), loop(right), op)

      case node @ Filter(_, joinSort @ (IdentitySort | ValueSort(_)), left, right) => 
        alg.Filter(node)(loop(left), loop(right))

      case node @ Operate(_, WrapArray, parent) =>
        alg.WrapArray(node)(loop(parent))

      case node @ Operate(_, op, parent) =>
        alg.Map1(node)(loop(parent), op)

      case node => alg.unmatched(node)
    }

    loop(to)
  }
}