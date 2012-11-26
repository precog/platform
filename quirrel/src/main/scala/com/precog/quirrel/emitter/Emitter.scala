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
import typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import bytecode.Instructions

import scalaz.{StateT, Id, Bind, Monoid}
import scalaz.Scalaz._

trait Emitter extends AST
    with Instructions
    with Binder
    with ProvenanceChecker
    with GroupSolver
    with Tracer {
      
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
  private case class MarkTicVar(let: ast.Solve, name: String) extends MarkType
  private case class MarkFormal(name: Identifier, let: ast.Let) extends MarkType
  private case class MarkDispatch(let: ast.Let, actuals: Vector[Expr]) extends MarkType
  private case class MarkGroup(op: ast.Where) extends MarkType

  private case class Emission(
    bytecode: Vector[Instruction] = Vector(),
    marks: Map[MarkType, Mark] = Map(),
    curLine: Option[(Int, String)] = None,
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
    val trace = buildTrace(Map())(expr)
    
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

    def emitLine(lineNum: Int, line: String): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      e.curLine match {
        case Some((`lineNum`, `line`)) => (e, ())

        case _ => emitInstr(Line(lineNum, line))(e.copy(curLine = Some((lineNum, line))))
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
        if (e.formals contains ((id, let)))
          (e, ())
        else
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
          val candidates: Set[List[ast.Dispatch]] = contextualDispatches(target)
          
          val dtracePrefix = dtrace.reverse
          
          val context = if (!(candidates forall { _.isEmpty })) {
            candidates map { _.reverse } find { c =>
              (c zip dtracePrefix takeWhile { case (a, b) => a == b } map { _._2 }) == dtracePrefix
            } get
          } else {
            Nil
          }
          
          emitBucketSpec(solve, forest, contextualDispatches, dispatches) >>
            prepareContext(context, dispatches) { dispatches => emitExpr(target, dispatches) } >>
            (origin map { labelGroup(_, id) } getOrElse mzero[EmitterState]) >>
            emitInstr(Group(id))
        }
      }
      
      case buckets.UnfixedSolution(name, solution) => {
        def state(id: Int) = {
          emitExpr(solution, dispatches) >>
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
      
      case buckets.FixedSolution(_, solution, expr) =>
        emitMap(solution, expr, Eq, dispatches) >> emitInstr(Extra)
      
      case buckets.Extra(expr) =>
        emitExpr(expr, dispatches) >> emitInstr(Extra)
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
      emitLine(expr.loc.lineNum, expr.loc.line) >>
      (expr match {
        case ast.Let(loc, id, params, left, right) =>
          emitExpr(right, dispatches)

        case expr @ ast.Solve(loc, _, body) => 
          val spec = expr.buckets(dispatches)
        
          val btraces: Map[Expr, Set[List[(Map[Formal, Expr], Expr)]]] =
            spec.exprs.map({ expr =>
              val btrace = buildBacktrace(trace)(expr)
              (expr -> btrace)
            })(collection.breakOut)
          
          val contextualDispatches: Map[Expr, Set[List[ast.Dispatch]]] = btraces map {
            case (key, pairPaths) => {
              val paths: Set[List[Expr]] = pairPaths map { pairs => pairs map { _._2 } }
              
              val innerDispatches = paths filter { _ contains expr } map { btrace =>
                btrace takeWhile (expr !=) collect {
                  case d: ast.Dispatch if d.binding.isInstanceOf[LetBinding] => d
                }
              }
              
              key -> innerDispatches
            }
          }
          
          emitBucketSpec(expr, spec, contextualDispatches, dispatches) >> 
            emitInstr(Split) >>
            emitExpr(body, dispatches) >>
            emitInstr(Merge)
                  
        
        case ast.Import(_, _, child) =>
          emitExpr(child, dispatches)

        case ast.New(loc, child) => 
          emitExpr(child, dispatches) >> emitInstr(Map1(New))
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          emitExpr(in, dispatches)
        
        case t @ ast.TicVar(loc, name) => { 
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
        
        case ast.StrLit(loc, value) => 
          emitInstr(PushString(value))
        
        case ast.NumLit(loc, value) => 
          emitInstr(PushNum(value))
        
        case ast.BoolLit(loc, value) => 
          emitInstr(value match {
            case true  => PushTrue
            case false => PushFalse
          })

        case ast.NullLit(loc) =>
          emitInstr(PushNull)
        
        case ast.ObjectDef(loc, Vector()) => emitInstr(PushObject)
        
        case ast.ObjectDef(loc, props) => 
          def field2ObjInstr(t: (String, Expr)) = emitInstr(PushString(t._1)) >> emitExpr(t._2, dispatches) >> emitInstr(Map2Cross(WrapObject))

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
          
        case ast.ArrayDef(loc, Vector()) => emitInstr(PushArray)

        case ast.ArrayDef(loc, values) => 
          val indexedValues = values.zipWithIndex

          val provToElements = indexedValues.groupBy(_._1.provenance)

          val (groups, indices) = provToElements.foldLeft((Vector.empty[EmitterState], Vector.empty[Int])) {
            case ((allStates, allIndices), (provenance, elements)) =>
              val singles = elements.map { case (expr, idx) => emitExpr(expr, dispatches) >> emitInstr(Map1(WrapArray)) }
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

              (if (currentIndex == targetIndex) (indices, state)
                else {
                  var (startIndex, endIndex) = if (currentIndex < targetIndex) (currentIndex, targetIndex) else (targetIndex, currentIndex)

                  val startValue = indices(startIndex)
                  val newIndices = indices.updated(startIndex, indices(endIndex)).updated(endIndex, startValue)

                  val newState = (startIndex until endIndex).foldLeft(state) {
                    case (state, idx) =>
                      state >> (emitInstr(PushNum(idx.toString)) >> emitInstr(Map2Cross(ArraySwap)))
                  }

                  (newIndices, newState)
                }
              , ())
          }

          val fixAll = (0 until indices.length).map(fixN)

          val fixedState = fixAll.foldLeft[StateT[Id, (Seq[Int], EmitterState), Unit]](StateT.stateT(()))(_ >> _).exec((indices, mzero[EmitterState]))._2

          joined >> fixedState
        
        case ast.Descent(loc, child, property) => 
          emitMapState(emitExpr(child, dispatches), child.provenance, emitInstr(PushString(property)), ValueProvenance, DerefObject)
        
        case ast.MetaDescent(loc, child, property) => 
          emitMapState(emitExpr(child, dispatches), child.provenance, emitInstr(PushString(property)), ValueProvenance, DerefMetadata)
        
        case ast.Deref(loc, left, right) => 
          emitMap(left, right, DerefArray, dispatches)
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case LoadBinding =>
              emitExpr(actuals.head, dispatches) >> emitInstr(LoadLocal)

            case DistinctBinding =>
              emitExpr(actuals.head, dispatches) >> emitInstr(Distinct)

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

            case LetBinding(let @ ast.Let(loc, id, params, left, right)) =>
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

        case ast.Cond(loc, pred, left, right) =>
           // if a then b else c
           // (b where a) union (c where !a)
           emitFilter(left, pred, dispatches) >> emitFilterState(emitExpr(right, dispatches), right.provenance, emitExpr(pred, dispatches) >> emitInstr(Map1(Comp)), pred.provenance) >> emitInstr(IUnion)
        
        case where @ ast.Where(_, _, _) =>
          emitWhere(where, dispatches)

        case ast.With(loc, left, right) =>
          emitMap(left, right, JoinObject, dispatches)

        case ast.Union(loc, left, right) =>
          emitExpr(left, dispatches) >> emitExpr(right, dispatches) >> emitInstr(IUnion)  

        case ast.Intersect(loc, left, right) =>
          emitExpr(left, dispatches) >> emitExpr(right, dispatches) >> emitInstr(IIntersect) 

        case ast.Difference(loc, left, right) =>
          emitExpr(left, dispatches) >> emitExpr(right, dispatches) >> emitInstr(SetDifference) 

        case ast.Add(loc, left, right) => 
          emitMap(left, right, Add, dispatches)
        
        case ast.Sub(loc, left, right) => 
          emitMap(left, right, Sub, dispatches)

        case ast.Mul(loc, left, right) => 
          emitMap(left, right, Mul, dispatches)
        
        case ast.Div(loc, left, right) => 
          emitMap(left, right, Div, dispatches)
        
        case ast.Mod(loc, left, right) => 
          emitMap(left, right, Mod, dispatches)
        
        case ast.Lt(loc, left, right) => 
          emitMap(left, right, Lt, dispatches)
        
        case ast.LtEq(loc, left, right) => 
          emitMap(left, right, LtEq, dispatches)
        
        case ast.Gt(loc, left, right) => 
          emitMap(left, right, Gt, dispatches)
        
        case ast.GtEq(loc, left, right) => 
          emitMap(left, right, GtEq, dispatches)
        
        case ast.Eq(loc, left, right) => 
          emitMap(left, right, Eq, dispatches)
        
        case ast.NotEq(loc, left, right) => 
          emitMap(left, right, NotEq, dispatches)
        
        case ast.Or(loc, left, right) => 
          emitMap(left, right, Or, dispatches)
        
        case ast.And(loc, left, right) =>
          emitMap(left, right, And, dispatches)
        
        case ast.Comp(loc, child) =>
          emitExpr(child, dispatches) >> emitInstr(Map1(Comp))
        
        case ast.Neg(loc, child) => 
          emitExpr(child, dispatches) >> emitInstr(Map1(Neg))
        
        case ast.Paren(loc, child) => 
          mzero[EmitterState]
      }) >> emitConstraints(expr, dispatches)
    }
    
    emitExpr(expr, Set()).exec(Emission()).bytecode
  }
}
