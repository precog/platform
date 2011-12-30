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

  private case class Mark(index: Int) { self =>
    def insert(insertIdx: Int, length: Int): Mark = copy(index = self.index + (if (insertIdx < index) length else 0))
  }

  private sealed trait MarkType
  private case class MarkExpr(expr: Expr) extends MarkType
  private case class MarkTicVar(let: ast.Let, name: String) extends MarkType

  private case class Emission private (
    bytecode: Vector[Instruction] = Vector.empty,
    ticVars:  Map[ast.Let, Seq[(String, EmitterState)]] = Map.empty,
    marks:    Map[MarkType, Mark] = Map.empty,
    curLine:  Option[(Int, String)] = None
  )
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateMonoid: Monoid[EmitterState] = new Monoid[EmitterState] {
    val zero = StateT.stateT[Id, Unit, Emission](())

    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private def reduce[A](xs: Iterable[A])(implicit m: Monoid[A]) = xs.foldLeft(mzero[A])(_ |+| _)

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

    def emitTicVar(let: ast.Let, name: String): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      if (e.marks.contains(MarkTicVar(let, name))) {
        // We've seen this tic var before, DUP it:
        emitDup(MarkTicVar(let, name))(e)
      } else {
        // Must emit bytecode for the tic var and mark it so we can DUP it
        // in the future:
        val state = e.ticVars(let).find(_._1 == name).map(_._2).get

        emitAndMark(MarkTicVar(let, name))(state)(e)
      }
    }

    def setTicVars(let: ast.Let, values: Seq[(String, EmitterState)])(f: => EmitterState): EmitterState = {
      applyTicVars(let, values) >> f >> unapplyTicVars(let)
    }

    def emitOrDup(markType: MarkType)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      if (e.marks.contains(markType)) emitDup(markType)(e) 
      else emitAndMark(markType)(f)(e)
    }

    def emitLine(lineNum: Int, line: String): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      e.curLine match {
        case Some((`lineNum`, `line`)) => ((), e)

        case _ => emitInstr(Line(lineNum, line))(e.copy(curLine = Some((lineNum, line))))
      }
    }

    private def operandStackSizes(is: Vector[Instruction]): Vector[Int] = {
      (is.foldLeft((Vector(0), 0)) {
        case ((vector, cur), instr) => 
          val delta = (instr.operandStackDelta._2 - instr.operandStackDelta._1)

          val total = cur + delta
          
          (vector :+ total, total)
      })._1
    }

    private def applyTicVars(let: ast.Let, values: Seq[(String, EmitterState)]): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      ((), e.copy(ticVars = e.ticVars + (let -> values)))
    }

    private def unapplyTicVars(let: ast.Let): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      ((), e.copy(ticVars = e.ticVars - let)) // TODO: Remove marks
    }

    // Emits the bytecode and marks it so it can be reused in DUPing operations.
    private def emitAndMark(markType: MarkType)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      f(e) match {
        case (_, e) =>
          val mark = Mark(e.bytecode.length)

          ((), e.copy(marks = e.marks + (markType -> mark)))
      }
    }

    // Dup's previously marked bytecode:
    private def emitDup(markType: MarkType): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val mark = e.marks(markType)

      val insertIdx = mark.index

      val stackSizes = operandStackSizes(e.bytecode)

      val insertStackSize = stackSizes(mark.index)
      val finalStackSize  = stackSizes(e.bytecode.length) + 1 // Add the DUP

      // Save the value by pushing it to the tail of the stack:
      val saveSwaps    = if (insertStackSize == 1) Vector.empty else (1 to insertStackSize).reverse.map(Swap.apply)

      // Restore the value by pulling it forward:
      val restoreSwaps = if (insertIdx == e.bytecode.length) Vector.empty else (1 until finalStackSize).map(Swap.apply)

      (insertInstrAt(Dup +: saveSwaps, insertIdx) >> 
      insertInstrAt(restoreSwaps, e.bytecode.length + 1 + saveSwaps.length)).apply(e)
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
      emitLine(expr.loc.lineNum, expr.loc.line) >>
      (expr match {
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

          reduce(groups ++ joins)

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

          val joined = reduce(groups ++ joins)

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
                  emitOrDup(MarkExpr(left))(emitExpr(left))

                case n =>
                  setTicVars(let, params.zip(actuals).map(t => t :-> emitExpr)) {
                    if (actuals.length == n) {
                      emitExpr(left)
                    } 
                    else {
                      val remainingParams = params.drop(actuals.length)

                      // Remove params already handled:
                      val filteredConditions = let.criticalConditions.toSeq.filter(t => remainingParams.contains(t._1))

                      // Sort by order of parameters:
                      val sortedConditions = filteredConditions.sortWith((a, b) => remainingParams.indexOf(a) < remainingParams.indexOf(b))

                      // Solve for each tic var:
                      val nameToSolutions: Seq[(String, Set[Expr])] = sortedConditions.map {
                        case (name, conditions) =>
                          (name, conditions.collect { case eq: ast.Eq => eq }.map { node =>
                            (solveRelation(node) { case ast.TicVar(_, `name`) => true }).
                              getOrElse[Expr](throw EmitterError(Some(node), "Cannot solve for " + name))
                          })
                      }

                      // Compute bytecode for every tic var:
                      val ticVarStates = nameToSolutions.map {
                        case (name, solutions) =>
                          val datasets = solutions.toSeq.map(emitExpr)
                          val unions   = Vector.fill(datasets.size - 1)(emitInstr(VUnion))

                          (name, reduce(datasets ++ unions) >> emitInstr(Split))
                      }

                      // At the end we have to merge everything back together:
                      val merges = reduce(Vector.fill(remainingParams.length)(emitInstr(Merge)))

                      setTicVars(let, ticVarStates) {
                        emitExpr(left) >> merges
                      }
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
      })
    }

    emitExpr(expr).exec(Emission.empty).bytecode
  }
}