package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.Solver
import com.querio.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.querio.bytecode.{Instructions}

import scalaz.{StateT, Id, Identity, Bind, Monoid}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with Solver with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Option[Expr], message: String) extends Exception(message)

  private def nullProvenanceError[A](): A = throw EmitterError(None, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(Some(expr), "Not implemented for expression type")

  private case class Mark(startIdx: Int, len: Int) { self =>
    def insert(idx2: Int, len2: Int) = copy(startIdx = self.startIdx + (if (idx2 <= startIdx) len2 else 0), len)

    def endIdx = startIdx + len
  }

  private case class Emission private (
    bytecode: Vector[Instruction] = Vector.empty,
    ticVars:  Map[ast.Let, Seq[(String, EmitterState)]] = Map.empty,
    marks:    Map[Expr, Mark] = Map.empty
  )

  private implicit val MarkSemigroup: Monoid[Mark] = new Monoid[Mark] {
    val zero = Mark(Int.MaxValue, 0)

    def append(v1: Mark, v2: => Mark) = {
      val min = v1.startIdx.min(v2.startIdx)
      val max = v1.endIdx.max(v2.endIdx)

      Mark(min, max - min)
    }
  }
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateMonoid: Monoid[EmitterState] = new Monoid[EmitterState] {
    val zero = StateT.stateT[Id, Unit, Emission](())

    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private object Emission {
    def empty = new Emission()

    def insertInstrAt(is: Seq[Instruction], _idx: Int): EmitterState = StateT.apply[Id, Emission, Unit] { e => 
      val idx = if (_idx < 0) (e.bytecode.length + 1 + _idx) else _idx

      val before = e.bytecode.take(idx)
      val after  = e.bytecode.drop(idx)

      ((), e.copy(bytecode = before ++ is ++ after, marks = e.marks.transform((k, v) => v.insert(idx, is.length))))
    }

    def insertInstrAt(i: Instruction, idx: Int): EmitterState = insertInstrAt(i :: Nil, idx)

    def emitInstr(i: Instruction): EmitterState = insertInstrAt(i, -1)

    def emitInstr(is: Seq[Instruction]): EmitterState = insertInstrAt(is, -1)

    private def operandStackSizes(is: Vector[Instruction]): Vector[Int] = {
      (is.foldLeft((Vector(0), 0)) {
        case ((vector, cur), instr) => 
          val delta = (instr.operandStackDelta._2 - instr.operandStackDelta._1)

          val total = cur + delta
          
          (vector :+ total, total)
      })._1
    }

    def operandStackSizeBefore(idx: Int): StateT[Id, Emission, Int] = StateT.apply[Id, Emission, Int] { e =>
      (operandStackSizes(e.bytecode)(idx), e)
    }

    def bytecodeLength: StateT[Id, Emission, Int] = StateT.apply[Id, Emission, Int] { e =>
      (e.bytecode.length, e)
    }

    def operandStackSize: StateT[Id, Emission, Int] = for { len <- bytecodeLength; size <- operandStackSizeBefore(len) } yield size

    private def applyTicVars(let: ast.Let, values: Seq[(String, EmitterState)]): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      ((), e.copy(ticVars = e.ticVars + (let -> values)))
    }

    private def unapplyTicVars(let: ast.Let): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      ((), e.copy(ticVars = e.ticVars - let))
    }

    def emitTicVar(let: ast.Let, name: String): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      e.ticVars(let).find(_._1 == name).get._2(e)
    }

    def setTicVars(let: ast.Let, values: Seq[(String, EmitterState)])(f: => EmitterState): EmitterState = {
      applyTicVars(let, values) >> f >> unapplyTicVars(let)
    }

    def emitAndMark(expr: Expr)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val markIdx = e.bytecode.length

      if (e.marks.contains(expr)) {
        emitMark(expr)(e)
      } else f(e) match {
        case (_, e) =>
          val len = e.bytecode.length - markIdx

          val mark = Mark(markIdx, len)

          ((), e.copy(marks = e.marks + (expr -> mark)))
      }
    }

    def emitMark(expr: Expr): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val mark = e.marks(expr)

      val insertIdx = mark.endIdx

      val stackSizes = operandStackSizes(e.bytecode)

      val insertStackSize = stackSizes(mark.endIdx)
      val finalStackSize  = stackSizes(e.bytecode.length) + 1 // Add the DUP

      // Save the value by pushing it to the tail of the stack:
      val saveSwaps    = if (insertStackSize == 1) Vector.empty else (1 to insertStackSize).reverse.map(Swap.apply)

      // Restore the value by pulling it forward:
      val restoreSwaps = if (insertIdx == e.bytecode.length) Vector.empty else (1 until finalStackSize).map(Swap.apply)

      (insertInstrAt(Dup +: saveSwaps, insertIdx) >> 
      insertInstrAt(restoreSwaps, e.bytecode.length + 1 + saveSwaps.length)).apply(e)
    }

    def findIndexOf(expr: EmitterState): StateT[Id, Emission, Int] = StateT.apply[Id, Emission, Int] { e =>
      val exprBytecode = expr.exec(Emission.empty).bytecode

      (e.bytecode.indexOfSlice(exprBytecode), e)
    }

    def instrLengthOf(expr: EmitterState): StateT[Id, Emission, Int] = StateT.apply[Id, Emission, Int] { e =>
      (expr.exec(Emission.empty).bytecode.length, e)
    }

    def instrLength: StateT[Id, Emission, Int] = StateT.apply[Id, Emission, Int] { e =>
      (e.bytecode.length, e)
    }
  }

  def emit(expr: Expr): Vector[Instruction] = {
    import Emission._

    def emitCrossOrMatchState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance)
        (ifCross: => Instruction, ifMatch: => Instruction): EmitterState = {
      val itx = leftProv.possibilities.intersect(rightProv.possibilities).filter(p => p != ValueProvenance && p != NullProvenance)

      val instr = emitInstr(if (itx.isEmpty) ifCross else ifMatch)

      left >> right >> instr
    }

    def emitMapState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance, op: BinaryOperation): EmitterState = {
      emitCrossOrMatchState(left, leftProv, right, rightProv)(
        ifCross = Map2Cross(op),
        ifMatch = Map2Match(op)
      )
    }

    def emitMap(left: Expr, right: Expr, op: BinaryOperation): EmitterState = {
      emitMapState(emitExpr(left), left.provenance, emitExpr(right), right.provenance, op)
    }

    def emitFilterState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance, depth: Short = 0, pred: Option[Predicate] = None): EmitterState = {
      emitCrossOrMatchState(left, leftProv, right, rightProv)(
        ifCross = FilterCross(depth, pred),
        ifMatch = FilterMatch(depth, pred)
      )
    }

    def emitFilter(left: Expr, right: Expr, depth: Short = 0, pred: Option[Predicate] = None): EmitterState = {
      emitFilterState(emitExpr(left), left.provenance, emitExpr(right), right.provenance, depth, pred)
    }

    def emitExpr(expr: Expr): StateT[Id, Emission, Unit] = {
      expr match {
        case ast.Let(loc, id, params, left, right) =>
          emitExpr(right)

        case ast.New(loc, child) => 
          mzero[EmitterState]
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          emitExpr(in)
        
        case t @ ast.TicVar(loc, name) => 
          t.binding match {
            case UserDef(let) =>
              emitTicVar(let, name)

            case _ => notImpl(expr)
          }
        
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
          emitMapState(emitExpr(child), child.provenance, emitInstr(PushString(property)), ValueProvenance, DerefObject)
        
        case ast.Deref(loc, left, right) => 
          emitMap(left, right, DerefArray)
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(LoadLocal(Het))

            case BuiltIn(BuiltIns.Count.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Count))
            
            case BuiltIn(BuiltIns.Max.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Max))
            
            case BuiltIn(BuiltIns.Mean.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Mean))
            
            case BuiltIn(BuiltIns.Median.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Median))
            
            case BuiltIn(BuiltIns.Min.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Min))
            
            case BuiltIn(BuiltIns.Mode.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Mode))
            
            case BuiltIn(BuiltIns.StdDev.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(StdDev))

            case BuiltIn(BuiltIns.Sum.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Sum))

            case BuiltIn(n, arity) =>
              notImpl(expr)

            case UserDef(let @ ast.Let(loc, id, params, left, right)) =>
              params.length match {
                case 0 =>
                  emitAndMark(left)(emitExpr(left))

                case n =>
                  setTicVars(let, params.zip(actuals).map(t => t :-> emitExpr)) {
                    if (actuals.length == n) {
                      emitExpr(left)
                    } 
                    else {
                      val remainingParams = params.drop(actuals.length)

                      val nameToSolutions: Map[String, Set[Expr]] = let.criticalConditions.filterKeys(remainingParams.contains _).transform {
                        case (name, conditions) =>
                          conditions.collect { case eq: ast.Eq => eq }.map { node =>
                            (solveRelation(node) { case ast.TicVar(_, _) => true }).
                              getOrElse[Expr](throw EmitterError(Some(node), "Cannot solve for " + name))
                          }
                      }

                      nameToSolutions.map {
                        case (name, solutions) =>
                          (name, solutions.map(emitExpr))
                      }

                      notImpl(expr)
                    } 
                  }
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

    emitExpr(expr).exec(Emission.empty).bytecode
  }
}