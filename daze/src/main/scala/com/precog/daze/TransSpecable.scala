package com.precog.daze

import com.precog.common.Path
import com.precog.common.json.{ CPathField, CPathIndex, CPathMeta }
import com.precog.yggdrasil._

import blueeyes.json._

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monadPlus._

trait TransSpecableModule[M[+_]] extends TransSpecModule with TableModule[M] with EvaluatorMethodsModule[M] {
  import dag._ 
  import library._
  import instructions._

  trait TransSpecable extends EvaluatorMethods {
    import trans._
    
    trait TransSpecableFold[T] {
      def EqualLiteral(node: Join)(parent: T, value: JValue, invert: Boolean): T
      def WrapObject(node: Join)(parent: T, field: String): T
      def DerefObjectStatic(node: Join)(parent: T, field: String): T
      def DerefMetadataStatic(node: Join)(parent: T, field: String): T
      def DerefArrayStatic(node: Join)(parent: T, index: Int): T
      def ArraySwap(node: Join)(parent: T, index: Int): T
      def InnerObjectConcat(node: Join)(parent: T): T
      def InnerArrayConcat(node: Join)(parent: T): T
      def Map1Left(node: Join)(parent: T, op: Op2, graph: DepGraph, value: JValue): T
      def Map1Right(node: Join)(parent: T, op: Op2, graph: DepGraph, value: JValue): T
      def binOp(node: Join)(leftParent: T, rightParent: => T, op: BinaryOperation): T
      def Filter(node: dag.Filter)(leftParent: T, rightParent: => T): T
      def WrapArray(node: Operate)(parent: T): T
      def Op1(node: Operate)(parent: T, op: UnaryOperation): T
      def unmatched(node: DepGraph): T
      def done(node: DepGraph): T
    }
    
    def isTransSpecable(to: DepGraph, from: DepGraph): Boolean = foldDownTransSpecable(to, Some(from))(new TransSpecableFold[Boolean] {
      def EqualLiteral(node: Join)(parent: Boolean, value: JValue, invert: Boolean) = parent
      def WrapObject(node: Join)(parent: Boolean, field: String) = parent
      def DerefObjectStatic(node: Join)(parent: Boolean, field: String) = parent
      def DerefMetadataStatic(node: Join)(parent: Boolean, field: String) = parent
      def DerefArrayStatic(node: Join)(parent: Boolean, index: Int) = parent
      def ArraySwap(node: Join)(parent: Boolean, index: Int) = parent
      def InnerObjectConcat(node: Join)(parent: Boolean) = parent
      def InnerArrayConcat(node: Join)(parent: Boolean) = parent
      def Map1Left(node: Join)(parent: Boolean, op: Op2, graph: DepGraph, value: JValue) = parent
      def Map1Right(node: Join)(parent: Boolean, op: Op2, graph: DepGraph, value: JValue) = parent
      def binOp(node: Join)(leftParent: Boolean, rightParent: => Boolean, op: BinaryOperation) = leftParent && rightParent
      def Filter(node: dag.Filter)(leftParent: Boolean, rightParent: => Boolean) = leftParent && rightParent
      def WrapArray(node: Operate)(parent: Boolean) = parent
      def Op1(node: Operate)(parent: Boolean, op: UnaryOperation) = parent
      def unmatched(node: DepGraph) = false
      def done(node: DepGraph) = true
    })
    
    private[this] def snd[A, B](a: A, b: B) = b
    
    def mkTransSpec(to: DepGraph, from: DepGraph, ctx: EvaluationContext) =
      mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, Some(from), ctx, identity, snd, some).map(_._1)  

    def findTransSpecAndAncestor(to: DepGraph, ctx: EvaluationContext) =
      mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, None, ctx, identity, snd, some)  
      
    def mkTransSpecWithState[N[+_] : Monad, S](to: DepGraph, from: Option[DepGraph], ctx: EvaluationContext, get: S => (TransSpec1, DepGraph), set: (S, (TransSpec1, DepGraph)) => S, init: ((TransSpec1, DepGraph)) => N[S]): N[S] = {
      foldDownTransSpecable(to, from)(new TransSpecableFold[N[S]] {
        import trans._

        // Bifunctor leftMap would be better here if it existed in pimped type inferrable form
        def leftMap(parent: S)(f: TransSpec1 => TransSpec1) = get(parent) match { 
          case (spec, ancestor) => set(parent, (f(spec), ancestor))
        }
        
        def EqualLiteral(node: Join)(parent: N[S], value: JValue, invert: Boolean) =
          parent.map(leftMap(_) { target =>
            val inner = trans.Equal(target, transJValue(value, target))
            if (invert) op1ForUnOp(Comp).spec(ctx)(inner) else inner
          })
          
        def WrapObject(node: Join)(parent: N[S], field: String) =
          parent.map(leftMap(_)(trans.WrapObject(_, field)))
          
        def DerefObjectStatic(node: Join)(parent: N[S], field: String) =
          parent.map(leftMap(_)(trans.DerefObjectStatic(_, CPathField(field))))
        
        def DerefMetadataStatic(node: Join)(parent: N[S], field: String) =
          parent.map(leftMap(_)(trans.DerefMetadataStatic(_, CPathMeta(field))))
        
        def DerefArrayStatic(node: Join)(parent: N[S], index: Int) =
          parent.map(leftMap(_)(trans.DerefArrayStatic(_, CPathIndex(index))))
        
        def ArraySwap(node: Join)(parent: N[S], index: Int) = 
          parent.map(leftMap(_)(trans.ArraySwap(_, index)))
        
        def InnerObjectConcat(node: Join)(parent: N[S]) =
          parent.map(leftMap(_)(trans.InnerObjectConcat(_)))
          
        def InnerArrayConcat(node: Join)(parent: N[S]) =
          parent.map(leftMap(_)(trans.InnerArrayConcat(_)))

        def Map1Left(node: Join)(parent: N[S], op: Op2, graph: DepGraph, value: JValue) =
          parent.map(leftMap(_) { target =>
            trans.Map2(target, transJValue(value, target), op.f2(ctx))
          })
          
        def Map1Right(node: Join)(parent: N[S], op: Op2, graph: DepGraph, value: JValue) =
          parent.map(leftMap(_) { target =>
            trans.Map2(transJValue(value, target), target, op.f2(ctx))
          })
        
        def binOp(node: Join)(leftParent: N[S], rightParent: => N[S], op: BinaryOperation) = {
          for {
            pl      <- leftParent
            (l, al) =  get(pl)
            pr      <- rightParent
            (r, ar) =  get(pr)
            result  <- if(al == ar) set(pl, (transFromBinOp(op, ctx)(l, r), al)).point[N] else init(Leaf(Source), node)  
          } yield result
        }
          
        def Filter(node: dag.Filter)(leftParent: N[S], rightParent: => N[S]) = 
          for {
            pl      <- leftParent
            (l, al) =  get(pl)
            pr      <- rightParent
            (r, ar) =  get(pr)
            result  <- if(al == ar) set(pl, (trans.Filter(l, r), al)).point[N] else init(Leaf(Source), node)
          } yield result
        
        def WrapArray(node: Operate)(parent: N[S]) =
          parent.map(leftMap(_)(trans.WrapArray(_)))
        
        def Op1(node: Operate)(parent: N[S], op: UnaryOperation) =
          parent.map(leftMap(_)(parent => op1ForUnOp(op).spec(ctx)(parent)))

        def unmatched(node: DepGraph) = init(Leaf(Source), node)
        
        def done(node: DepGraph) = init(Leaf(Source), node)
      })
    }

    def foldDownTransSpecable[T](to: DepGraph, from: Option[DepGraph])(alg: TransSpecableFold[T]): T = {
      
      object ConstInt {
        def unapply(c: Const) = c match {
          case Const(JNum(n)) => Some(n.toInt)
          case Const(JNumLong(n)) => Some(n.toInt)
          case Const(JNumDouble(n)) => Some(n.toInt)
          case _ => None
        }
      }
      
      object Op2ForBinOp {
        def unapply(op: BinaryOperation) = op2ForBinOp(op)
      }

      def loop(graph: DepGraph): T = graph match {
        case node if from.map(_ == node).getOrElse(false) => alg.done(node)
          
        case node @ Join(Eq, CrossLeftSort | CrossRightSort, left, Const(value)) =>
          alg.EqualLiteral(node)(loop(left), value, false)

        case node @ Join(Eq, CrossLeftSort | CrossRightSort, Const(value), right) =>
          alg.EqualLiteral(node)(loop(right), value, false)

        case node @ Join(NotEq, CrossLeftSort | CrossRightSort, left, Const(value)) =>
          alg.EqualLiteral(node)(loop(left), value, true)

        case node @ Join(NotEq, CrossLeftSort | CrossRightSort, Const(value), right) =>
          alg.EqualLiteral(node)(loop(right), value, true)

        case node @ Join(instructions.WrapObject, CrossLeftSort | CrossRightSort, Const(JString(field)), right) =>
          alg.WrapObject(node)(loop(right), field)

        case node @ Join(DerefObject, CrossLeftSort | CrossRightSort, left, Const(JString(field))) =>
          alg.DerefObjectStatic(node)(loop(left), field)
        
        case node @ Join(DerefMetadata, CrossLeftSort | CrossRightSort, left, Const(JString(field))) =>
          alg.DerefMetadataStatic(node)(loop(left), field)

        case node @ Join(DerefArray, CrossLeftSort | CrossRightSort, left, ConstInt(index)) =>
          alg.DerefArrayStatic(node)(loop(left), index)
        
        case node @ Join(instructions.ArraySwap, CrossLeftSort | CrossRightSort, left, ConstInt(index)) =>
          alg.ArraySwap(node)(loop(left), index)

        case node @ Join(JoinObject, CrossLeftSort | CrossRightSort, left, Const(JObject.empty)) =>
          alg.InnerObjectConcat(node)(loop(left))
                    
        case node @ Join(JoinObject, CrossLeftSort | CrossRightSort, Const(JObject.empty), right) =>
          alg.InnerObjectConcat(node)(loop(right))

        case node @ Join(JoinArray, CrossLeftSort | CrossRightSort, left, Const(JArray.empty)) =>
          alg.InnerArrayConcat(node)(loop(left))
                    
        case node @ Join(JoinArray, CrossLeftSort | CrossRightSort, Const(JArray.empty), right) =>
          alg.InnerArrayConcat(node)(loop(right))

        case node @ Join(Op2ForBinOp(op), CrossLeftSort | CrossRightSort, left, Const(value)) =>
          alg.Map1Left(node)(loop(left), op, left, value)

        case node @ Join(Op2ForBinOp(op), CrossLeftSort | CrossRightSort, Const(value), right) =>
          alg.Map1Right(node)(loop(right), op, right, value) 

        case node @ Join(op, joinSort @ (IdentitySort | ValueSort(_)), left, right) => 
          alg.binOp(node)(loop(left), loop(right), op)

        case node @ dag.Filter(joinSort @ (IdentitySort | ValueSort(_)), left, right) => 
          alg.Filter(node)(loop(left), loop(right))

        case node @ Operate(instructions.WrapArray, parent) =>
          alg.WrapArray(node)(loop(parent))

        case node @ Operate(op, parent) =>
          alg.Op1(node)(loop(parent), op)

        case node => alg.unmatched(node)
      }

      loop(to)
    }
  }
}
