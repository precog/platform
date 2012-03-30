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
package com.precog.quirrel.emitter

import com.precog.quirrel.parser.AST
import com.precog.quirrel.Solver
import com.precog.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.precog.bytecode.{Instructions}

import scalaz.{StateT, Id, Identity, Bind, Monoid}
import scalaz.Scalaz._

trait Emitter extends AST
    with Instructions
    with Binder
    with ProvenanceChecker
    with GroupSolver {
      
  import instructions._
  
  case class EmitterError(expr: Option[Expr], message: String) extends Exception(message)
  
  private def nullProvenanceError[A](): A = throw EmitterError(None, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(Some(expr), "Not implemented for expression type")

  private case class Mark(index: Int, offset: Int) { self =>
    def insert(insertIdx: Int, length: Int): Mark =
      copy(index = self.index + (if (insertIdx < index) length else 0))
  }

  private sealed trait MarkType
  private case class MarkExpr(expr: Expr) extends MarkType
  private case class MarkTicVar(let: ast.Let, name: String) extends MarkType
  private case class MarkDispatch(let: ast.Let, actuals: Vector[Expr]) extends MarkType
  private case class MarkGroup(op: ast.Where) extends MarkType

  private case class Emission private (
    bytecode: Vector[Instruction] = Vector(),
    marks: Map[MarkType, Mark] = Map(),
    curLine: Option[(Int, String)] = None,
    buckets: Map[ast.Where, Set[Expr]] = Map(),
    toDrop: List[Set[MarkType]] = Nil)
  
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

      ((), e.copy(
        bytecode = before ++ is ++ after,
        marks = e.marks.transform((k, v) => v.insert(idx, is.length))))
    }

    def insertInstrAt(i: Instruction, idx: Int): EmitterState = insertInstrAt(i :: Nil, idx)

    def emitInstr(i: Instruction): EmitterState = insertInstrAt(i, -1)

    def emitOrDup(markType: MarkType)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      if (e.marks.contains(markType))
        emitDup(markType)(e) 
      else
        emitAndMark(markType)(f)(e)
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

    // Emits the bytecode and marks it so it can be reused in DUPing operations.
    private def emitAndMark(markType: MarkType)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      f(e) match {
        case (_, e) =>
          val mark = Mark(e.bytecode.length, 0)
        
          ((), e.copy(marks = e.marks + (markType -> mark)))
      }
    }
    
    private def markTicVar(let: ast.Let, name: String, offset: Int): EmitterState = {
      val mark = MarkTicVar(let, name)
      
      val markState = StateT.apply[Id, Emission, Unit] { e =>
        ((), e.copy(marks = e.marks + (mark -> Mark(e.bytecode.length, offset))))
      }
      
      markState >> markForDrop(mark)
    }
    
    private def markAllGroups(bucket: Bucket, offset: Int, seen: Set[ast.Where]): (EmitterState, Int, Set[ast.Where]) = bucket match {
      case UnionBucket(left, right) => {
        val (state1, offset1, seen1) = markAllGroups(left, offset, seen)
        val (state2, offset2, seen2) = markAllGroups(right, offset1, seen1)
        (state1 >> state2, offset2, seen2)
      }
      
      case IntersectBucket(left, right) => {
        val (state1, offset1, seen1) = markAllGroups(left, offset, seen)
        val (state2, offset2, seen2) = markAllGroups(right, offset1, seen1)
        (state1 >> state2, offset2, seen2)
      }
      
      case Group(origin, _, _, extras) if !seen(origin) => {
        val mark = MarkGroup(origin)
        
        val state = StateT.apply[Id, Emission, Unit] { e =>
          val e2 = e.copy(
            marks = e.marks + (mark -> Mark(e.bytecode.length, offset)),
            buckets = e.buckets + (origin -> extras))
            
          ((), e2)
        }
        
        (state >> markForDrop(mark), offset + 1, seen + origin)
      }
      
      case _ => (mzero[EmitterState], offset, seen)
    }
    
    private def pushDropFrame: EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      ((), e.copy(toDrop = Set[MarkType]() :: e.toDrop))
    }
    
    private def markForDrop(markType: MarkType): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val newDrop = e.toDrop match {
        case hd :: tail => (hd + markType) :: tail
        case Nil => Nil
      }
      
      ((), e.copy(toDrop = newDrop))
    }
    
    /**
     * Do ''not'' use this state unless there is exactly one value at the head of
     * the stack with the entire drop frame below it.
     */
    private def popDropFrame: EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val (optSet, newDrop) = e.toDrop match {
        case hd :: tail => (Some(hd), tail)
        case Nil => (None, Nil)
      }
      
      val results = optSet map { _ map { _ => emitInstr(Swap(1)) >> emitInstr(Drop) } } getOrElse Set(mzero[EmitterState])
      
      val back = reduce(results) >> (StateT.apply[Id, Emission, Unit] { e =>
        ((), e.copy(toDrop = newDrop))
      })
      
      back(e)
    }
    
    private def emitDrop(markType: MarkType): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val Mark(insertIdx, offset) = e.marks(markType)

      val stackSizes = operandStackSizes(e.bytecode)
      val targetStackSize = stackSizes(insertIdx)
      val finalStackSize  = stackSizes(e.bytecode.length)
      
      val distance = finalStackSize - targetStackSize + 1
      
      // Restore the value by pulling it forward:
      val swaps = if (distance == 1)
        Vector()
      else
        (1 until distance) map Swap
      
      ((), e.copy(bytecode = e.bytecode ++ swaps :+ Drop))
    }
    
    // Dup's previously marked bytecode:
    private def emitDup(markType: MarkType): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val Mark(insertIdx, offset) = e.marks(markType)

      val stackSizes = operandStackSizes(e.bytecode)

      val insertStackSize = stackSizes(insertIdx)
      val finalStackSize  = stackSizes(e.bytecode.length) + 1 // Add the DUP
      
      val pullUp = (1 to offset) map Swap
      
      val pushDown = if (offset > 0)
        (1 to (offset + 1)).reverse map Swap
      else
        Vector()

      // Save the value by pushing it to the tail of the stack:
      val saveSwaps = if (insertStackSize == 1)
        Vector()
      else
        (1 to insertStackSize).reverse map Swap

      // Restore the value by pulling it forward:
      val restoreSwaps = if (finalStackSize == 1)
        Vector()
      else
        (1 until finalStackSize) map Swap

      (insertInstrAt((pullUp :+ Dup) ++ pushDown ++ saveSwaps, insertIdx) >> 
        insertInstrAt(restoreSwaps, e.bytecode.length + pullUp.length + 1 + pushDown.length + saveSwaps.length))(e)
    }
    
    def emitConstraints(expr: Expr): EmitterState = {
      val optState = for (const <- expr.constrainingExpr if !(const equalsIgnoreLoc expr)) yield {
        if (expr.children exists { _.constrainingExpr == Some(const) })
          None
        else {
          Some(emitExpr(const) >> emitInstr(Dup) >> emitInstr(Map2Match(Eq)) >> emitInstr(FilterMatch(0, None)))
        }
      }
      
      optState flatMap identity getOrElse mzero[EmitterState]
    }
    
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

    def emitUnary(expr: Expr, op: UnaryOperation): EmitterState = {
      emitExpr(expr) >> emitInstr(Map1(op))
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
    
    def emitWhere(where: ast.Where): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val ast.Where(loc, left, right) = where
      
      val state = if (e.buckets contains where) {
        val extras = e.buckets(where)
        
        // TODO unify the provenances to discover if we need to cross
        
        if (extras.isEmpty) {
          emitDup(MarkGroup(where))
        } else {
          val extraStates = extras map { e => (emitExpr(e), e.provenance) }
          
          val Some((state, prov)) = extraStates.foldLeft(None: Option[(EmitterState, Provenance)]) {
            case (Some((leftState, leftProv)), (rightState, rightProv)) => {
              Some((emitCrossOrMatchState(leftState, leftProv, rightState, rightProv)(
                ifCross = Map2Cross(And),
                ifMatch = Map2Match(And)), unifyProvenanceAssumingRelated(leftProv, rightProv)))
            }
            
            case (None, pair) => Some(pair)
          }
          
          emitCrossOrMatchState(emitDup(MarkGroup(where)), where.provenance, state, prov)(
            ifCross = FilterCross(0, None),
            ifMatch = FilterMatch(0, None))
        }
      } else {
        emitFilter(left, right, 0, None)
      }
      
      state(e)
    }
    
    def origins(bucket: Bucket): Set[ast.Where] = bucket match {
      case UnionBucket(left, right) => origins(left) ++ origins(right)
      case IntersectBucket(left, right) => origins(left) ++ origins(right)
      case Group(origin, _, _, _) => Set(origin)
    }
    
    def emitBucket(bucket: Bucket): EmitterState = bucket match {
      case UnionBucket(left, right) =>
        emitBucket(left) >> emitBucket(right) >> emitInstr(ZipBuckets)      // can this ever happen?
      
      case IntersectBucket(left, right) =>
        emitBucket(left) >> emitBucket(right) >> emitInstr(ZipBuckets)
      
      case Group(_, target, forest, _) => emitSolution(target, forest)
    }
    
    // er...?
    def emitSolution(target: Expr, solution: Solution): EmitterState = solution match {
      case Disjunction(left, right) =>
        emitSolution(target, left) >> emitSolution(target, right) >> emitInstr(MergeBuckets(false))
      
      case Conjunction(left, right) =>
        emitSolution(target, left) >> emitSolution(target, right) >> emitInstr(MergeBuckets(true))
      
      case Definition(expr) => emitExpr(target) >> emitExpr(expr) >> emitInstr(Bucket)
    }
    
    def emitExpr(expr: Expr): StateT[Id, Emission, Unit] = {
      emitLine(expr.loc.lineNum, expr.loc.line) >>
      (expr match {
        case ast.Let(loc, id, params, left, right) =>
          emitExpr(right)

        case ast.New(loc, child) => 
          emitExpr(child) >> emitInstr(Map1(New))
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          emitExpr(in)
        
        case t @ ast.TicVar(loc, name) => 
          t.binding match {
            case UserDef(let) => emitDup(MarkTicVar(let, name))
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
            case BuiltIn(BuiltIns.Load.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(LoadLocal(Het))

            case BuiltIn(BuiltIns.Count.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Count))
            
            case BuiltIn(BuiltIns.GeometricMean.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(GeometricMean))
            
            case BuiltIn(BuiltIns.Max.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Max))
            
            case BuiltIn(BuiltIns.Mean.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Mean))
            
            case BuiltIn(BuiltIns.Median.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Median))
            
            case BuiltIn(BuiltIns.Min.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Min))
            
            case BuiltIn(BuiltIns.Mode.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Mode))
            
            case BuiltIn(BuiltIns.StdDev.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(StdDev))

            case BuiltIn(BuiltIns.Sum.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Sum))

            case BuiltIn(BuiltIns.SumSq.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(SumSq))

            case BuiltIn(BuiltIns.Variance.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(Reduce(Variance))

            case BuiltIn(BuiltIns.Distinct.name, arity, _) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(SetReduce(Distinct))

            case BuiltIn(n, arity, _) =>
              notImpl(expr)

            case StdlibBuiltIn1(op) =>
              emitUnary(actuals(0), BuiltInFunction1Op(op))

            case StdlibBuiltIn2(op) =>
              emitMap(actuals(0), actuals(1), BuiltInFunction2Op(op))

            case UserDef(let @ ast.Let(loc, id, params, left, right)) =>
              params.length match {
                case 0 =>
                  emitOrDup(MarkExpr(left))(emitExpr(left))

                case n => emitOrDup(MarkDispatch(let, actuals)) {
                  val (actualStates, actualMarks) = params zip actuals map {
                    case (name, expr) => {
                      val mark = MarkTicVar(let, name)
                      (emitAndMark(mark)(emitExpr(expr)), mark)
                    }
                  } unzip
                  
                  val populateActualFrame = StateT.apply[Id, Emission, Unit] { e =>
                    val toDrop2 = e.toDrop match {
                      case hd :: tl => (hd ++ actualMarks) :: tl
                      case Nil => Nil
                    }
                    
                    ((), e.copy(toDrop = toDrop2))
                  }
                  
                  val body = if (actuals.length == n) {
                    pushDropFrame >>
                      populateActualFrame >>
                      emitExpr(left) >>
                      popDropFrame
                  } else {
                    val (buckets, bucketStates) = d.buckets.toSeq map {
                      case pair @ (name, bucket) => (pair, emitBucket(bucket))
                    } unzip

                    val n = buckets.length.toShort
                    val k = (buckets.length + (buckets map { _._2 } map origins map { _.size } sum)).toShort
                    val split = emitInstr(Split(n, k))
                    
                    /*
                     * Split
                     *   <group22>
                     *   <group21>
                     *   <var2>
                     *   <group11>
                     *   <var1>
                     */
                    
                    val (groups, _) = buckets.foldLeft((mzero[EmitterState], 0)) {
                      case ((state, offset), (name, bucket)) => {
                        val (state2, offset2, _) = markAllGroups(bucket, offset + 1, Set())
                        (markTicVar(let, name, offset) >> state2 >> state, offset2)
                      }
                    }
                    
                    reduce(bucketStates) >>
                      split >>
                      pushDropFrame >>
                      populateActualFrame >>
                      groups >>
                      emitExpr(left) >>
                      popDropFrame >>
                      emitInstr(Merge)
                  }
                  
                  reduce(actualStates) >> body
                }
              }

            case NullBinding => 
              notImpl(expr)
          }
        
        case where @ ast.Where(_, _, _) =>
          emitWhere(where)

        case ast.With(loc, left, right) =>
          emitMap(left, right, JoinObject)

        case ast.Union(loc, left, right) =>
          emitExpr(left) >> emitExpr(right) >> emitInstr(IUnion)  

        case ast.Intersect(loc, left, right) =>
          emitExpr(left) >> emitExpr(right) >> emitInstr(IIntersect) 

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
          mzero[EmitterState]
      }) >> emitConstraints(expr)
    }
  }

  def emit(expr: Expr): Vector[Instruction] = {
    import Emission._
    emitExpr(expr).exec(Emission.empty).bytecode
  }
}
