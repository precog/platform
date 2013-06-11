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
package com.precog
package quirrel
package emitter

import parser.AST
import typer.{Binder, ProvenanceChecker}
import bytecode.Instructions

import scalaz.{StateT, Id, Bind, Monoid, NonEmptyList => NEL}
import scalaz.Scalaz._

trait Emitter extends AST
    with Instructions
    with Binder
    with ProvenanceChecker
    with GroupSolver
    with Tracer {
      
  import instructions._
  import library._

  case class EmitterError(expr: Option[Expr], message: String) extends Exception(message)
  
  private def nullProvenanceError[A](): A = throw EmitterError(None, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(Some(expr), "Not implemented for expression type")

  private case class Mark(index: Int, offset: Int) { self =>
    def insert(insertIdx: Int, length: Int): Mark =
      copy(index = self.index + (if (insertIdx < index) length else 0))
  }

  private sealed trait MarkType
  private case class MarkExpr(expr: Expr) extends MarkType
  private case class MarkTicVar(let: ast.Solve, name: String) extends MarkType
  private case class MarkFormal(name: Identifier, let: ast.Let) extends MarkType
  private case class MarkDispatch(let: ast.Let, actuals: Vector[Expr]) extends MarkType
  private case class MarkGroup(op: ast.Where) extends MarkType

  private case class Emission(
    bytecode: Vector[Instruction] = Vector(),
    marks: Map[MarkType, Mark] = Map(),
    lineStack: List[(Int, Int, String)] = Nil,
    ticVars: Map[(ast.Solve, TicId), EmitterState] = Map(),
    keyParts: Map[(ast.Solve, TicId), Int] = Map(),
    formals: Map[(Identifier, ast.Let), EmitterState] = Map(),
    groups: Map[ast.Where, Int] = Map(),
    subResolve: Provenance => Provenance = identity,
    currentId: Int = 0)
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateMonoid: Monoid[EmitterState] = new Monoid[EmitterState] {
    val zero = StateT.stateT[Id, Emission, Unit](())

    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private def reduce[A](xs: Iterable[A])(implicit m: Monoid[A]) = xs.foldLeft(mzero[A])(_ |+| _)

  def emit(expr: Expr): Vector[Instruction] = {
    lazy val trace = expr.trace

    def insertInstrAtMulti(is: Seq[Instruction], _idx: Int): EmitterState = StateT.apply[Id, Emission, Unit] { e => 
      val idx = if (_idx < 0) (e.bytecode.length + 1 + _idx) else _idx

      val before = e.bytecode.take(idx)
      val after  = e.bytecode.drop(idx)

      (e.copy(
        bytecode = before ++ is ++ after,
        marks = e.marks.transform((k, v) => v.insert(idx, is.length))), ())
    }

    def insertInstrAt(i: Instruction, idx: Int): EmitterState = insertInstrAtMulti(i :: Nil, idx)
    
    def nextId(f: Int => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      f(e.currentId)(e.copy(currentId = e.currentId + 1))
    }

    def emitInstr(i: Instruction): EmitterState = insertInstrAt(i, -1)
    
    def emitOrDup(markType: MarkType)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      if (e.marks.contains(markType))
        emitDup(markType)(e) 
      else
        emitAndMark(markType)(f)(e)
    }

    def emitLine(line: Int, col: Int, text: String): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val e2 = e.copy(lineStack = (line, col, text) :: e.lineStack)
      
      e.lineStack match {
        case (`line`, `col`, `text`) :: _ => (e2, ())

        case stack =>
          emitInstr(Line(line, col, text))(e2)
      }
    }
    
    def emitPopLine: EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      e.lineStack match {
        case Nil => (e, ())
        
        case _ :: Nil => (e.copy(lineStack = Nil), ())
        
        case _ :: (stack @ (line, col, text) :: _) =>
          emitInstr(Line(line, col, text))(e.copy(lineStack = stack))
      }
    }

    def operandStackSizes(is: Vector[Instruction]): Vector[Int] = {
      (is.foldLeft((Vector(0), 0)) {
        case ((vector, cur), instr) => 
          val delta = (instr.operandStackDelta._2 - instr.operandStackDelta._1)

          val total = cur + delta
          
          (vector :+ total, total)
      })._1
    }

    // Emits the bytecode and marks it so it can be reused in DUPing operations.
    def emitAndMark(markType: MarkType)(f: => EmitterState): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      f(e) match {
        case (e, _) =>
          val mark = Mark(e.bytecode.length, 0)
        
          (e.copy(marks = e.marks + (markType -> mark)), ())
      }
    }
    
    def labelTicVar(solve: ast.Solve, name: TicId)(state: => EmitterState): EmitterState = {
      StateT.apply[Id, Emission, Unit] { e =>
        (e.copy(ticVars = e.ticVars + ((solve, name) -> state)), ())
      }
    }
    
    def labelFormal(id: Identifier, let: ast.Let)(state: => EmitterState): EmitterState = {
      StateT.apply[Id, Emission, Unit] { e =>
        (e.copy(formals = e.formals + ((id, let) -> state)), ())
      }
    }

    def labelGroup(where: ast.Where, id: Int): EmitterState = {
      StateT.apply[Id, Emission, Unit] { e =>
        (e.copy(groups = e.groups + (where -> id)), ())
      }
    }
    
    // Dup's previously marked bytecode:
    def emitDup(markType: MarkType): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
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

      (insertInstrAtMulti((pullUp :+ Dup) ++ pushDown ++ saveSwaps, insertIdx) >> 
        insertInstrAtMulti(restoreSwaps, e.bytecode.length + pullUp.length + 1 + pushDown.length + saveSwaps.length))(e)
    }
    
    def emitConstraints(expr: Expr, dispatches: Set[ast.Dispatch]): EmitterState = {
      val optState = for (const <- expr.constrainingExpr if !(const equalsIgnoreLoc expr)) yield {
        if (expr.children exists { _.constrainingExpr == Some(const) })
          None
        else {
          Some(emitExpr(const, dispatches) >> emitInstr(Dup) >> emitInstr(Map2Match(Eq)) >> emitInstr(FilterMatch))
        }
      }
      
      optState flatMap identity getOrElse mzero[EmitterState]
    }
    
    def emitCrossOrMatchState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance)
        (ifCross: => Instruction, ifMatch: => Instruction): EmitterState = {
      StateT.apply[Id, Emission, Unit] { e =>
        val leftResolved = e.subResolve(leftProv)
        val rightResolved = e.subResolve(rightProv)
        
        assert(!leftResolved.isParametric)
        assert(!rightResolved.isParametric)
        
        val itx = leftResolved.possibilities.intersect(rightResolved.possibilities).filter(p => p != ValueProvenance && p != NullProvenance)
  
        val instr = emitInstr(if (itx.isEmpty) ifCross else ifMatch)
  
        (left >> right >> instr)(e)
      }
    }

    def emitMapState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance, op: BinaryOperation): EmitterState = {
      emitCrossOrMatchState(left, leftProv, right, rightProv)(
        ifCross = Map2Cross(op),
        ifMatch = Map2Match(op)
      )
    }

    def emitMap(left: Expr, right: Expr, op: BinaryOperation, dispatches: Set[ast.Dispatch]): EmitterState = {
      emitMapState(emitExpr(left, dispatches), left.provenance, emitExpr(right, dispatches), right.provenance, op)
    }

    def emitUnary(expr: Expr, op: UnaryOperation, dispatches: Set[ast.Dispatch]): EmitterState = {
      emitExpr(expr, dispatches) >> emitInstr(Map1(op))
    }

    def emitFilterState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance): EmitterState = {
      emitCrossOrMatchState(left, leftProv, right, rightProv)(
        ifCross = FilterCross,
        ifMatch = FilterMatch
      )
    }

    def emitFilter(left: Expr, right: Expr, dispatches: Set[ast.Dispatch]): EmitterState = {
      emitFilterState(emitExpr(left, dispatches), left.provenance, emitExpr(right, dispatches), right.provenance)
    }
    
    def emitWhere(where: ast.Where, dispatches: Set[ast.Dispatch]): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val ast.Where(loc, left, right) = where
      
      val state = if (e.groups contains where) {
        val id = e.groups(where)
        emitOrDup(MarkGroup(where))(emitInstr(PushGroup(id)))
      } else {
        emitFilter(left, right, dispatches)
      }
      
      state(e)
    }
    
    def emitBucketSpec(solve: ast.Solve, spec: BucketSpec, contextualDispatches: Map[Expr, Set[List[ast.Dispatch]]], dispatches: Set[ast.Dispatch]): EmitterState = spec match {
      case buckets.UnionBucketSpec(left, right) =>
        emitBucketSpec(solve, left, contextualDispatches, dispatches) >> emitBucketSpec(solve, right, contextualDispatches, dispatches) >> emitInstr(MergeBuckets(false))
      
      case buckets.IntersectBucketSpec(left, right) =>
        emitBucketSpec(solve, left, contextualDispatches, dispatches) >> emitBucketSpec(solve, right, contextualDispatches, dispatches) >> emitInstr(MergeBuckets(true))
      
      case buckets.Group(origin, target, forest, dtrace) => {
        nextId { id =>
          val context = generateContext(target, contextualDispatches, dtrace)
          
          emitBucketSpec(solve, forest, contextualDispatches, dispatches) >>
            prepareContext(context, dispatches) { dispatches => emitExpr(target, dispatches) } >>
            (origin map { labelGroup(_, id) } getOrElse mzero[EmitterState]) >>
            emitInstr(Group(id))
        }
      }
      
      case buckets.UnfixedSolution(name, solution, dtrace) => {
        val context = generateContext(solution, contextualDispatches, dtrace)
        
        def state(id: Int) = {
          prepareContext(context, dispatches) { dispatches => emitExpr(solution, dispatches) } >>
            labelTicVar(solve, name)(emitInstr(PushKey(id))) >>
            emitInstr(KeyPart(id))
        }
        
        StateT.apply[Id, Emission, Unit] { e =>
          val s = if (e.keyParts contains (solve -> name))
            state(e.keyParts(solve -> name))
          else {
            nextId { id =>
              state(id) >>
                (StateT.apply[Id, Emission, Unit] { e =>
                  (e.copy(keyParts = e.keyParts + ((solve, name) -> id)), ())
                })
            }
          }
          
          s(e)
        }
      }
      
      case buckets.FixedSolution(_, solution, expr, dtrace) => {
        val context = generateContext(solution, contextualDispatches, dtrace)
        
        prepareContext(context, dispatches) { dispatches => emitMap(solution, expr, Eq, dispatches) } >> emitInstr(Extra)
      }
      
      case buckets.Extra(expr, dtrace) => {
        val context = generateContext(expr, contextualDispatches, dtrace)
        
        prepareContext(context, dispatches) { dispatches => emitExpr(expr, dispatches) } >> emitInstr(Extra)
      }
    }
    
    def generateContext(target: Expr, contextualDispatches: Map[Expr, Set[List[ast.Dispatch]]], dtrace: List[ast.Dispatch]) = {
      val candidates: Set[List[ast.Dispatch]] = contextualDispatches(target)
      val dtracePrefix = dtrace.reverse
      
      if (!(candidates forall { _.isEmpty })) {
        candidates map { _.reverse } find { c =>
          (c zip dtracePrefix takeWhile { case (a, b) => a == b } map { _._2 }) == dtracePrefix
        } getOrElse Nil
      } else {
        Nil
      }
    }
    
    def prepareContext(context: List[ast.Dispatch], dispatches: Set[ast.Dispatch])(f: Set[ast.Dispatch] => EmitterState): EmitterState = context match {
      case d :: tail => {
        d.binding match {
          case LetBinding(let) => {
            emitDispatch(d, let, dispatches) { dispatches =>
              prepareContext(tail, dispatches)(f)
            }
          }
          
          case _ => prepareContext(tail, dispatches)(f)
        }
      }
      
      case Nil => f(dispatches)
    }

    def createJoins(provs: NEL[Provenance], op2: BinaryOperation): (Set[Provenance], Vector[EmitterState]) = {
      // if `provs` has size one, we should not emit a join instruction
      provs.tail.foldLeft((provs.head.possibilities, Vector.empty[EmitterState])) {
        case ((provAcc, instrAcc), prov) => {
          val joinInstr = StateT.apply[Id, Emission, Unit] { e =>
            val resolvedProv = e.subResolve(prov)
            val resolvedProvAcc = provAcc.map(e.subResolve)

            assert(!resolvedProv.isParametric)

            val intersection = resolvedProv.possibilities.intersect(resolvedProvAcc)
            val itx = intersection.filter(p => p != ValueProvenance && p != NullProvenance)

            emitInstr(if (itx.isEmpty) Map2Cross(op2) else Map2Match(op2))(e)
          }

          val resultProv = provAcc ++ prov.possibilities
          (resultProv, instrAcc :+ joinInstr)
        }
      }
    }
    
    def emitDispatch(expr: ast.Dispatch, let: ast.Let, dispatches: Set[ast.Dispatch])(f: Set[ast.Dispatch] => EmitterState): EmitterState = {
      val ast.Dispatch(_, name, actuals) = expr
      val ast.Let(_, _, params, left, right) = let
      
      val ids = let.params map { Identifier(Vector(), _) }
      val zipped = ids zip (actuals map { _.provenance })
      
      def sub(target: Provenance): Provenance = {
        zipped.foldLeft(target) {
          case (target, (id, sub)) => substituteParam(id, let, target, sub)
        }
      }
      
      val actualStates = params zip actuals map {
        case (name, expr) =>
          labelFormal(Identifier(Vector(), name), let)(emitExpr(expr, dispatches))
      }
      
      StateT.apply[Id, Emission, Unit] { e =>
        def subResolve2(prov: Provenance): Provenance =
          resolveUnifications(expr.relations)(sub(prov))
        
        val e2 = e.copy(subResolve = e.subResolve compose subResolve2)
        
        val (e3, ()) = (reduce(actualStates) >> f(dispatches + expr))(e2)
        val e4 = e3.copy(formals = params.foldLeft(e3.formals)((fs, name) => fs - ((Identifier(Vector(), name), let))))
        val e5 = e4.copy(marks = params.foldLeft(e4.marks)((fs, name) => fs - (MarkFormal(Identifier(Vector(), name), let))))
        (e5.copy(subResolve = e.subResolve), ())
      }
    }
    
    def emitExpr(expr: Expr, dispatches: Set[ast.Dispatch]): StateT[Id, Emission, Unit] = {
      emitLine(expr.loc.lineNum, expr.loc.colNum, expr.loc.line) >>
      (expr match {
        case ast.Let(loc, id, params, left, right) =>
          emitExpr(right, dispatches)

        case expr @ ast.Solve(loc, _, body) =>
          val spec = expr.buckets(dispatches)
        
          val btraces: Map[Expr, List[List[(Sigma, Expr)]]] =
            spec.exprs.map({ expr =>
              val btrace = buildBacktrace(trace)(expr)
              (expr -> btrace)
            })(collection.breakOut)
          
          val contextualDispatches: Map[Expr, Set[List[ast.Dispatch]]] = btraces map {
            case (key, pairPaths) => {
              val paths: List[List[Expr]] = pairPaths map { pairs => pairs map { _._2 } }
              
              val innerDispatches = paths filter { _ contains expr } map { btrace =>
                btrace takeWhile (expr !=) collect {
                  case d: ast.Dispatch if d.binding.isInstanceOf[LetBinding] => d
                }
              } toSet
              
              key -> innerDispatches
            }
          }
          
          emitBucketSpec(expr, spec, contextualDispatches, dispatches) >> 
            emitInstr(Split) >>
            emitExpr(body, dispatches) >>
            emitInstr(Merge)

        case ast.Import(_, _, child) =>
          emitExpr(child, dispatches)
        
        case ast.Assert(_, pred, child) =>
          emitExpr(pred, dispatches) >> emitExpr(child, dispatches) >> emitInstr(Assert)

        case ast.Observe(_, data, samples) =>
          emitExpr(data, dispatches) >> emitExpr(samples, dispatches) >> emitInstr(Observe)

        case ast.New(_, child) => 
          emitExpr(child, dispatches) >> emitInstr(Map1(New))
        
        case ast.Relate(_, _, _, in) =>
          emitExpr(in, dispatches)
        
        case t @ ast.TicVar(_, name) => { 
          t.binding match {
            case SolveBinding(solve) => {
              emitOrDup(MarkTicVar(solve, name)) {
                StateT.apply[Id, Emission, Unit] { e =>
                  e.ticVars((solve, name))(e)     // assert: this will work iff lexical scoping is working
                }
              }
            }
            
            case _ => notImpl(expr)
          }
        }
        
        case ast.StrLit(_, value) => 
          emitInstr(PushString(value))
        
        case ast.NumLit(_, value) => 
          emitInstr(PushNum(value))
        
        case ast.BoolLit(_, value) => 
          emitInstr(value match {
            case true  => PushTrue
            case false => PushFalse
          })

        case ast.NullLit(_) =>
          emitInstr(PushNull)
        
        case ast.UndefinedLit(_) =>
          emitInstr(PushUndefined)

        case ast.ObjectDef(_, Vector()) =>
          emitInstr(PushObject)
        
        case ast.ObjectDef(_, props) => {
          def fieldToObjInstr(t: (String, Expr)) =
            emitInstr(PushString(t._1)) >> emitExpr(t._2, dispatches) >> emitInstr(Map2Cross(WrapObject))

          val provToField = props.groupBy(_._2.provenance).toList.sortBy { case (p, _) => p }(Provenance.order.toScalaOrdering)
          val provs = provToField map { case (p, _) => p } reverse

          val groups = provToField.foldLeft(Vector.empty[EmitterState]) {
            case (stateAcc, (provenance, fields)) =>
              val singles = fields.map(fieldToObjInstr)

              val joinInstr = StateT.apply[Id, Emission, Unit] { e =>
                val resolved = e.subResolve(provenance)
                emitInstr(if (resolved == ValueProvenance) Map2Cross(JoinObject) else Map2Match(JoinObject))(e)
              }

              val joins = Vector.fill(singles.length - 1)(joinInstr)

              stateAcc ++ (singles ++ joins)
          }

          val (_, joins) = createJoins(NEL(provs.head, provs.tail: _*), JoinObject)

          reduce(groups ++ joins)
        }

        case ast.ArrayDef(_, Vector()) =>
          emitInstr(PushArray)

        case ast.ArrayDef(_, values) => {
          val indexedValues = values.zipWithIndex
          
          val provToElements = indexedValues.groupBy(_._1.provenance).toList.sortBy { case (p, _) => p }(Provenance.order.toScalaOrdering)
          val provs = provToElements map { case (p, _) => p } reverse

          val (groups, indices) = provToElements.foldLeft((Vector.empty[EmitterState], Vector.empty[Int])) {
            case ((allStates, allIndices), (provenance, elements)) => {
              val singles = elements.map { case (expr, _) => emitExpr(expr, dispatches) >> emitInstr(Map1(WrapArray)) }
              val indices = elements.map(_._2)

              val joinInstr = StateT.apply[Id, Emission, Unit] { e =>
                val resolved = e.subResolve(provenance)
                emitInstr(if (resolved == ValueProvenance) Map2Cross(JoinArray) else Map2Match(JoinArray))(e)
              }

              val joins = Vector.fill(singles.length - 1)(joinInstr)

              (allStates ++ (singles ++ joins), allIndices ++ indices)
            }
          }

          val (_, joins) = createJoins(NEL(provs.head, provs.tail: _*), JoinArray)

          val joined = reduce(groups ++ joins)

          def resolve(remap: Map[Int, Int])(i: Int): Int =
            remap get i map resolve(remap) getOrElse i

          val (_, swaps) = indices.zipWithIndex.foldLeft((Map[Int, Int](), mzero[EmitterState])) {
            case ((remap, state), (j, i)) if resolve(remap)(i) != j => {
              // swap(i')
              // swap(j)
              // swap(i')

              val i2 = resolve(remap)(i)

              val state2 = {
                if (j == 0) {  //required for correctness
                  emitInstr(PushNum(i2.toString)) >> emitInstr(Map2Cross(ArraySwap))
                } else if (i2 == 0) {  //not required for correctness, but nice simplification
                  emitInstr(PushNum(j.toString)) >> emitInstr(Map2Cross(ArraySwap))
                } else {
                  val swps = (i2 :: j :: i2 :: Nil)
                  swps map { idx => emitInstr(PushNum(idx.toString)) >> emitInstr(Map2Cross(ArraySwap)) } reduce { _ >> _ }
                }
              }

              (remap + (j -> i2), state >> state2)
            }

            case (pair, _) => pair
          }

          joined >> swaps
        }
        
        case ast.Descent(_, child, property) => 
          emitMapState(emitExpr(child, dispatches), child.provenance, emitInstr(PushString(property)), ValueProvenance, DerefObject)
        
        case ast.MetaDescent(_, child, property) => 
          emitMapState(emitExpr(child, dispatches), child.provenance, emitInstr(PushString(property)), ValueProvenance, DerefMetadata)
        
        case ast.Deref(_, left, right) => 
          emitMap(left, right, DerefArray, dispatches)
        
        case d @ ast.Dispatch(_, name, actuals) => {
          d.binding match {
            case LoadBinding =>
              emitExpr(actuals.head, dispatches) >> emitInstr(AbsoluteLoad)
            
            case RelLoadBinding =>
              emitExpr(actuals.head, dispatches) >> emitInstr(RelativeLoad)

            case DistinctBinding =>
              emitExpr(actuals.head, dispatches) >> emitInstr(Distinct)

            case ExpandGlobBinding => 
              emitExpr(actuals.head, dispatches) >> emitInstr(Morph1(BuiltInMorphism1(expandGlob)))

            case Morphism1Binding(m) => 
              emitExpr(actuals.head, dispatches) >> emitInstr(Morph1(BuiltInMorphism1(m)))

            case Morphism2Binding(m) => 
              emitExpr(actuals(0), dispatches) >> emitExpr(actuals(1), dispatches) >> emitInstr(Morph2(BuiltInMorphism2(m)))

            case ReductionBinding(f) =>
              emitExpr(actuals.head, dispatches) >> emitInstr(Reduce(BuiltInReduction(f)))

            case FormalBinding(let) =>
              emitOrDup(MarkFormal(name, let)) {
                StateT.apply[Id, Emission, Unit] { e =>
                  e.formals((name, let))(e)
                }
              }

            case Op1Binding(op) =>  
              emitUnary(actuals(0), BuiltInFunction1Op(op), dispatches)

            case Op2Binding(op) =>
              emitMap(actuals(0), actuals(1), BuiltInFunction2Op(op), dispatches)

            case LetBinding(let @ ast.Let(_, id, params, left, right)) =>
              if (params.length > 0) {
                emitOrDup(MarkDispatch(let, actuals)) {
                  // Don't let dispatch add marks inside the function - could mark expressions that include formals
                  // Run the StateT and preserve its marks
                  StateT.apply[Id, Emission, Unit] { e =>
                    val state = emitDispatch(d, let, dispatches) { dispatches =>
                      emitExpr(left, dispatches)
                    }
                    val (e2, ()) = state(e)
                    (e2.copy(marks = e.marks), ())
                  }
                }
              } else {
                emitOrDup(MarkExpr(left))(emitExpr(left, dispatches + d))
              }

            case NullBinding => 
              notImpl(expr)
          }
        }

        case ast.Cond(_, pred, left, right) =>
           // if a then b else c
           // (b where a) union (c where !a)
           emitFilter(left, pred, dispatches) >> emitFilterState(emitExpr(right, dispatches), right.provenance, emitExpr(pred, dispatches) >> emitInstr(Map1(Comp)), pred.provenance) >> emitInstr(IUnion)
        
        case where @ ast.Where(_, _, _) =>
          emitWhere(where, dispatches)

        case ast.With(_, left, right) =>
          emitMap(left, right, JoinObject, dispatches)

        case ast.Union(_, left, right) =>
          emitExpr(left, dispatches) >> emitExpr(right, dispatches) >> emitInstr(IUnion)  

        case ast.Intersect(_, left, right) =>
          emitExpr(left, dispatches) >> emitExpr(right, dispatches) >> emitInstr(IIntersect) 

        case ast.Difference(_, left, right) =>
          emitExpr(left, dispatches) >> emitExpr(right, dispatches) >> emitInstr(SetDifference) 

        case ast.Add(_, left, right) => 
          emitMap(left, right, Add, dispatches)
        
        case ast.Sub(_, left, right) => 
          emitMap(left, right, Sub, dispatches)

        case ast.Mul(_, left, right) => 
          emitMap(left, right, Mul, dispatches)
        
        case ast.Div(_, left, right) => 
          emitMap(left, right, Div, dispatches)
        
        case ast.Mod(_, left, right) => 
          emitMap(left, right, Mod, dispatches)
        
        case ast.Pow(_, left, right) =>
          emitMap(left, right, Pow, dispatches)

        case ast.Lt(_, left, right) => 
          emitMap(left, right, Lt, dispatches)
        
        case ast.LtEq(_, left, right) => 
          emitMap(left, right, LtEq, dispatches)
        
        case ast.Gt(_, left, right) => 
          emitMap(left, right, Gt, dispatches)
        
        case ast.GtEq(_, left, right) => 
          emitMap(left, right, GtEq, dispatches)
        
        case ast.Eq(_, left, right) => 
          emitMap(left, right, Eq, dispatches)
        
        case ast.NotEq(_, left, right) => 
          emitMap(left, right, NotEq, dispatches)
        
        case ast.Or(_, left, right) => 
          emitMap(left, right, Or, dispatches)
        
        case ast.And(_, left, right) =>
          emitMap(left, right, And, dispatches)
        
        case ast.Comp(_, child) =>
          emitExpr(child, dispatches) >> emitInstr(Map1(Comp))
        
        case ast.Neg(_, child) => 
          emitExpr(child, dispatches) >> emitInstr(Map1(Neg))
        
        case ast.Paren(_, child) => 
          emitExpr(child, dispatches)
      }) >> emitConstraints(expr, dispatches) >> emitPopLine
    }
    
    collapseLines(emitExpr(expr, Set()).exec(Emission()).bytecode)
  }
  
  private def collapseLines(bytecode: Vector[Instruction]): Vector[Instruction] = {
    bytecode.foldLeft(Vector[Instruction]()) {
      case (acc, line: Line) if !acc.isEmpty && acc.last.isInstanceOf[Line] =>
        acc.updated(acc.length - 1, line)
      
      case (acc, instr) => acc :+ instr
    }
  }
}
