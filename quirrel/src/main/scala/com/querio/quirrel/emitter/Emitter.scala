package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.querio.bytecode.{Instructions}

import scalaz.{StateT, Id, Identity, Bind, Semigroup}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Expr, message: String) extends Exception(message)

  private def nullProvenanceError[A](expr: Expr): A = throw EmitterError(expr, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(expr, "Not implemented")

  private case class Emission(
    bytecode: Vector[Instruction] = Vector.empty
  )

  // This doesn't seem to work
  /*private implicit def BindSemigroup[M[_], A](implicit bind: Bind[M]): Semigroup[M[A]] = new Semigroup[M[A]] {
    def append(v1: M[A], v2: => M[A]): M[A] = bind.bind(v1)((a: A) => v2)
  }*/
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateSemigroup: Semigroup[EmitterState] = new Semigroup[EmitterState] {
    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private object Emission {
    def emitInstr(i: Instruction): EmitterState = StateT.apply[Id, Emission, Unit](e => (Unit, e.copy(bytecode = e.bytecode :+ i)))

    def emitInstrs(is: Seq[Instruction]): EmitterState = StateT.apply[Id, Emission, Unit](e => (Unit, e.copy(bytecode = e.bytecode ++ is)))
  }

  def emit(expr: Expr): Vector[Instruction] = {
    import Emission._

    def emitExprBinary(left: Expr, right: Expr, op: BinaryOperation): EmitterState = {
      (left.provenance, right.provenance) match {
        case (NullProvenance, _) => 
          nullProvenanceError(left)

        case (_, NullProvenance) => 
          nullProvenanceError(right)

        case (p1, p2) =>
          val bytecode = (p1, p2) match {
            case (StaticProvenance(p1), StaticProvenance(p2)) if (p1 == p2) => 
              Map2Match(op)
            
            case (DynamicProvenance(id1), DynamicProvenance(id2)) if (id1 == id2) =>
              Map2Match(op)

            case (ValueProvenance, p2) if (p2 != ValueProvenance) =>
              Map2CrossRight(op)

            case (p1, ValueProvenance) if (p1 != ValueProvenance) =>
              Map2CrossLeft(op)

            case (_, _) =>
              // TODO: Not correct in general
              Map2Cross(op)
          }

          emitExpr(left) >> emitExpr(right) >> emitInstr(bytecode)
      }
    }

    def emitExpr(expr: Expr): StateT[Id, Emission, Unit] = {
      expr match {
        case ast.Let(loc, id, params, left, right) =>
          params.length match {
            case 0 =>
              emitExpr(right)

            case n =>
              notImpl(expr)
          }

        case ast.New(loc, child) => 
          notImpl(expr)
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          notImpl(expr)
        
        case t @ ast.TicVar(loc, id) => 
          notImpl(expr)
        
        case ast.StrLit(loc, value) => 
          emitInstr(PushString(value))
        
        case ast.NumLit(loc, value) => 
          emitInstr(PushNum(value))
        
        case ast.BoolLit(loc, value) => 
          emitInstr(value match {
            case true  => PushTrue
            case false => PushFalse
          })
        
        case ast.ObjectDef(loc, props) => 
          def field2ObjInstr(t: (String, Expr)) = emitInstr(PushString(t._1)) >> emitExpr(t._2) >> emitInstr(Map2Cross(WrapObject))

          // TODO: Non-constants
          val singles = props.map(field2ObjInstr)
          val joins   = Vector.fill(props.length - 1)(emitInstr(Map2Cross(JoinObject)))

          (singles ++ joins).foldLeft[EmitterState](StateT.stateT[Id, Unit, Emission](()))(_ |+| _)
        
        case ast.ArrayDef(loc, values) => 
          notImpl(expr)
        
        case ast.Descent(loc, child, property) => 
          notImpl(expr)
        
        case ast.Deref(loc, left, right) => 
          notImpl(expr)
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(LoadLocal(Het))

            case BuiltIn(n, arity) =>
              notImpl(expr)

            case UserDef(ast.Let(loc, id, params, left, right)) =>
              emitExpr(left)

            case NullBinding => 
              notImpl(expr)
          }
        
        case ast.Operation(loc, left, op, right) => 
          // WHERE clause -- to be refactored (!)
          op match {
            case "where" => 
              emitExpr(left) >> emitExpr(right) >> {
                (left.provenance, right.provenance) match {
                  case (p1, p2) if (p1 == p2 & p1 != ValueProvenance) => emitInstr(FilterMatch(0, None))

                  case _ => emitInstr(FilterCross(0, None))
                }
              }

            case _ => notImpl(expr)
          }
        
        case ast.Add(loc, left, right) => 
          emitExprBinary(left, right, Add)
        
        case ast.Sub(loc, left, right) => 
          emitExprBinary(left, right, Sub)

        case ast.Mul(loc, left, right) => 
          emitExprBinary(left, right, Mul)
        
        case ast.Div(loc, left, right) => 
          emitExprBinary(left, right, Div)
        
        case ast.Lt(loc, left, right) => 
          emitExprBinary(left, right, Lt)
        
        case ast.LtEq(loc, left, right) => 
          emitExprBinary(left, right, LtEq)
        
        case ast.Gt(loc, left, right) => 
          emitExprBinary(left, right, Gt)
        
        case ast.GtEq(loc, left, right) => 
          emitExprBinary(left, right, GtEq)
        
        case ast.Eq(loc, left, right) => 
          emitExprBinary(left, right, Eq)
        
        case ast.NotEq(loc, left, right) => 
          emitExprBinary(left, right, NotEq)
        
        case ast.Or(loc, left, right) => 
          emitExprBinary(left, right, Or)
        
        case ast.And(loc, left, right) =>
          emitExprBinary(left, right, And)
        
        case ast.Comp(loc, child) =>
          emitExpr(child) >> emitInstr(Map1(Comp))
        
        case ast.Neg(loc, child) => 
          emitExpr(child) >> emitInstr(Map1(Neg))
        
        case ast.Paren(loc, child) => 
          StateT.stateT[Id, Unit, Emission](Unit)
      }
    }

    emitExpr(expr).exec(Emission()).bytecode
  }
}