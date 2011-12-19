package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.querio.bytecode.{Instructions}

import scalaz.{StateT, Id, Identity, Bind, Monoid}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Expr, message: String) extends Exception(message)

  private def nullProvenanceError[A](expr: Expr): A = throw EmitterError(expr, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(expr, "Not implemented")

  private case class Emission(
    bytecode: Vector[Instruction] = Vector.empty
  )
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateMonoid: Monoid[EmitterState] = new Monoid[EmitterState] {
    val zero = StateT.stateT[Id, Unit, Emission](())

    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private object Emission {
    def emitInstr(i: Instruction): EmitterState = StateT.apply[Id, Emission, Unit](e => ((), e.copy(bytecode = e.bytecode :+ i)))

    def emitInstr(is: Seq[Instruction]): EmitterState = StateT.apply[Id, Emission, Unit](e => ((), e.copy(bytecode = e.bytecode ++ is)))

    def operandStackSize(idx: Int): StateT[Id, Emission, Int] = StateT.apply[Id, Emission, Int] { e =>
      // TODO: This should really go into a Bytecode abstraction, which should hold Vector[Instruction]
      val operandStackSize = e.bytecode.take(idx).foldLeft(0) {
        case (size, instr) => size + (instr.operandStackDelta._2 - instr.operandStackDelta._1)
      }

      (operandStackSize, e)
    }

    // TODO: This should really use metadata inside Emission
    def dupOrAppend(expr: EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      // Get the bytecode for the expression:
      val exprBytecode = expr.exec(Emission()).bytecode

      // Look for the expression bytecode in the emission bytecode:
      e.bytecode.indexOfSlice(exprBytecode) match {
        case -1 =>
          // If it's not found, then simply emit the bytecode here:
          ((), e.copy(bytecode = e.bytecode ++ exprBytecode))

        case start =>
          // The expression bytecode was found. Need to duplicate it and 
          // transform the instruction stack so the DUP has no effect:
          val idx = start + exprBytecode.length
          
          val beforeStackSize = operandStackSize(idx).eval(e)   
          val finalStackSize  = operandStackSize(e.bytecode.length).eval(e) + 1 // Add the DUP

          val before = e.bytecode.take(idx)
          val after  = e.bytecode.drop(idx)

          // Operation preserving transform -- note, if the stack only has one element,
          // there's no need to perform a swap since it doesn't materially change 
          // anything -- whatever was on the stack was DUP'd.
          val swaps = if (beforeStackSize == 1) Vector.empty else (1 to beforeStackSize).reverse.map(Swap.apply)

          // There may be some final swaps at the end to move up the DUP:
          val finalSwap = (1 until finalStackSize).map(Swap.apply)

          ((), e.copy(bytecode = (before :+ Dup) ++ swaps ++ after ++ finalSwap))
      }
    }
  }

  def emit(expr: Expr): Vector[Instruction] = {
    import Emission._

    def emitMap(left: Expr, right: Expr, op: BinaryOperation): EmitterState = {
      val mapInstr = emitInstr((left.provenance, right.provenance) match {
        case (NullProvenance, _) =>
          nullProvenanceError(left)

        case (_, NullProvenance) =>
          nullProvenanceError(right)

        case (StaticProvenance(p1), StaticProvenance(p2)) if (p1 == p2) => 
          Map2Match(op)
        
        case (DynamicProvenance(id1), DynamicProvenance(id2)) if (id1 == id2) =>
          Map2Match(op)

        case (ValueProvenance, p2) if (p2 != ValueProvenance) =>
          Map2CrossRight(op)

        case (p1, ValueProvenance) if (p1 != ValueProvenance) =>
          Map2CrossLeft(op)

        case (_, _) =>
          Map2Cross(op)
      })

      emitExpr(left) >> emitExpr(right) >> mapInstr
    }

    def emitFilter(left: Expr, right: Expr, depth: Short = 0, pred: Option[Predicate] = None): EmitterState = {
      val filterInstr = emitInstr((left.provenance, right.provenance) match {
        case (NullProvenance, _) =>
          nullProvenanceError(left)

        case (_, NullProvenance) =>
          nullProvenanceError(right)

        case (StaticProvenance(p1), StaticProvenance(p2)) if (p1 == p2) => 
          FilterMatch(depth, pred)
        
        case (DynamicProvenance(id1), DynamicProvenance(id2)) if (id1 == id2) =>
          FilterMatch(depth, pred)

        case (_, _) =>
          FilterCross(depth, pred)
      })

      emitExpr(left) >> emitExpr(right) >> filterInstr
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

          val provToField = props.groupBy(_._2.provenance)

          // TODO: The fields are not in the right order because of the group operation
          val groups = provToField.foldLeft(Vector.empty[EmitterState]) {
            case (vector, (provenance, fields)) =>
              val singles = fields.map(field2ObjInstr)

              val joinInstr = emitInstr(if (provenance == ValueProvenance) Map2Cross(JoinObject) else Map2Match(JoinObject))

              val joins = Vector.fill(singles.length - 1)(joinInstr)

              vector ++ (singles ++ joins)
          }

          val joins = Vector.fill(provToField.size - 1)(emitInstr(Map2Cross(JoinObject)))

          (groups ++ joins).foldLeft(mzero[EmitterState])(_ >> _)

        case ast.ArrayDef(loc, values) => 
          val indexedValues = values.zipWithIndex

          val provToElements = indexedValues.groupBy(_._1.provenance)

          val (groups, indices) = provToElements.foldLeft((Vector.empty[EmitterState], Vector.empty[Int])) {
            case ((allStates, allIndices), (provenance, elements)) =>
              val singles = elements.map { case (expr, idx) => emitExpr(expr) >> emitInstr(Map1(WrapArray)) }
              val indices = elements.map(_._2)

              val joinInstr = emitInstr(if (provenance == ValueProvenance) Map2Cross(JoinArray) else Map2Match(JoinArray))

              val joins = Vector.fill(singles.length - 1)(joinInstr)

              (allStates ++ (singles ++ joins), allIndices ++ indices)
          }

          val joins = Vector.fill(provToElements.size - 1)(emitInstr(Map2Cross(JoinArray)))

          val joined = (groups ++ joins).foldLeft(mzero[EmitterState])(_ >> _)

          // This function takes a list of indices and a state, and produces
          // a new list of indices and a new state, where the Nth index of the
          // array will be moved into the correct location.
          def fixN(n: Int): StateT[Id, (Seq[Int], EmitterState), Unit] = StateT.apply[Id, (Seq[Int], EmitterState), Unit] { 
            case (indices, state) =>
              val currentIndex = indices.indexOf(n)
              val targetIndex  = n

              ((), 
              if (currentIndex == targetIndex) (indices, state)
              else {
                var (startIndex, endIndex) = if (currentIndex < targetIndex) (currentIndex, targetIndex) else (targetIndex, currentIndex)

                val startValue = indices(startIndex)
                val newIndices = indices.updated(startIndex, indices(endIndex)).updated(endIndex, startValue)

                val newState = (startIndex until endIndex).foldLeft(state) {
                  case (state, idx) =>
                    state >> (emitInstr(PushNum(idx.toString)) >> emitInstr(Map2Cross(ArraySwap)))
                }

                (newIndices, newState)
              })
          }

          val fixAll = (0 until indices.length).map(fixN)

          val fixedState = fixAll.foldLeft[StateT[Id, (Seq[Int], EmitterState), Unit]](StateT.stateT(()))(_ >> _).exec((indices, mzero[EmitterState]))._2

          joined >> fixedState
        
        case ast.Descent(loc, child, property) => 
          // Object
          // TODO: Why is "property" not expression???????
          emitExpr(child) >> emitInstr(PushString(property)) >> emitInstr(Map2Cross(DerefObject))
        
        case ast.Deref(loc, left, right) => 
          emitMap(left, right, DerefArray)
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(LoadLocal(Het))

            case BuiltIn(n, arity) =>
              notImpl(expr)

            case UserDef(ast.Let(loc, id, params, left, right)) =>
              params.length match {
                case 0 =>
                  dupOrAppend(emitExpr(left))

                case n =>
                  notImpl(expr)
              }

            case NullBinding => 
              notImpl(expr)
          }
        
        case ast.Operation(loc, left, op, right) => 
          // WHERE clause -- to be refactored (!)
          op match {
            case "where" => 
              emitFilter(left, right, 0, None)

            case _ => notImpl(expr)
          }
        
        case ast.Add(loc, left, right) => 
          emitMap(left, right, Add)
        
        case ast.Sub(loc, left, right) => 
          emitMap(left, right, Sub)

        case ast.Mul(loc, left, right) => 
          emitMap(left, right, Mul)
        
        case ast.Div(loc, left, right) => 
          emitMap(left, right, Div)
        
        case ast.Lt(loc, left, right) => 
          emitMap(left, right, Lt)
        
        case ast.LtEq(loc, left, right) => 
          emitMap(left, right, LtEq)
        
        case ast.Gt(loc, left, right) => 
          emitMap(left, right, Gt)
        
        case ast.GtEq(loc, left, right) => 
          emitMap(left, right, GtEq)
        
        case ast.Eq(loc, left, right) => 
          emitMap(left, right, Eq)
        
        case ast.NotEq(loc, left, right) => 
          emitMap(left, right, NotEq)
        
        case ast.Or(loc, left, right) => 
          emitMap(left, right, Or)
        
        case ast.And(loc, left, right) =>
          emitMap(left, right, And)
        
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