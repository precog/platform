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
    ticVars: Map[(ast.Let, TicId), EmitterState] = Map(),
    groups: Map[ast.Where, Int] = Map(),
    currentId: Int = 0)
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateMonoid: Monoid[EmitterState] = new Monoid[EmitterState] {
    val zero = StateT.stateT[Id, Emission, Unit](())

    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private def reduce[A](xs: Iterable[A])(implicit m: Monoid[A]) = xs.foldLeft(mzero[A])(_ |+| _)

  private object Emission {
    def empty = new Emission()
    
    def insertInstrAt(is: Seq[Instruction], _idx: Int): EmitterState = StateT.apply[Id, Emission, Unit] { e => 
      val idx = if (_idx < 0) (e.bytecode.length + 1 + _idx) else _idx

      val before = e.bytecode.take(idx)
      val after  = e.bytecode.drop(idx)

      (e.copy(
        bytecode = before ++ is ++ after,
        marks = e.marks.transform((k, v) => v.insert(idx, is.length))), ())
    }

    def insertInstrAt(i: Instruction, idx: Int): EmitterState = insertInstrAt(i :: Nil, idx)
    
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
        case (e, _) =>
          val mark = Mark(e.bytecode.length, 0)
        
          (e.copy(marks = e.marks + (markType -> mark)), ())
      }
    }
    
    private def labelTicVar(let: ast.Let, name: TicId)(state: => EmitterState): EmitterState = {
      StateT.apply[Id, Emission, Unit] { e =>
        (e.copy(ticVars = e.ticVars + ((let, name) -> state)), ())
      }
    }
    
    private def labelGroup(where: ast.Where, id: Int): EmitterState = {
      StateT.apply[Id, Emission, Unit] { e =>
        (e.copy(groups = e.groups + (where -> id)), ())
      }
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
          Some(emitExpr(const) >> emitInstr(Dup) >> emitInstr(Map2Match(Eq)) >> emitInstr(FilterMatch))
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
      // emitMapState(emitExpr(left), left.provenance, emitExpr(right), right.provenance, op)
      notImpl(left)
    }

    def emitUnary(expr: Expr, op: UnaryOperation): EmitterState = {
      emitExpr(expr) >> emitInstr(Map1(op))
    }

    def emitFilterState(left: EmitterState, leftProv: Provenance, right: EmitterState, rightProv: Provenance): EmitterState = {
      emitCrossOrMatchState(left, leftProv, right, rightProv)(
        ifCross = FilterCross,
        ifMatch = FilterMatch
      )
    }

    def emitFilter(left: Expr, right: Expr): EmitterState = {
      // emitFilterState(emitExpr(left), left.provenance, emitExpr(right), right.provenance)
      notImpl(left)
    }
    
    def emitWhere(where: ast.Where): EmitterState = StateT.apply[Id, Emission, Unit] { e =>
      val ast.Where(loc, left, right) = where
      
      val state = if (e.groups contains where) {
        val id = e.groups(where)
        emitOrDup(MarkGroup(where))(emitInstr(PushGroup(id)))
      } else {
        emitFilter(left, right)
      }
      
      state(e)
    }
    
    def emitBucketSpec(let: ast.Let, spec: BucketSpec): EmitterState = spec match {
      case buckets.UnionBucketSpec(left, right) =>
        emitBucketSpec(let, left) >> emitBucketSpec(let, right) >> emitInstr(MergeBuckets(false))
      
      case buckets.IntersectBucketSpec(left, right) =>
        emitBucketSpec(let, left) >> emitBucketSpec(let, right) >> emitInstr(MergeBuckets(true))
      
      case buckets.Group(origin, target, forest) => {
        nextId { id =>
          emitBucketSpec(let, forest) >>
            emitExpr(target) >>
            labelGroup(origin, id) >>
            emitInstr(Group(id))
        }
      }
      
      case buckets.UnfixedSolution(name, solution) => {
        nextId { id =>
          emitExpr(solution) >>
            labelTicVar(let, name)(emitInstr(PushKey(id))) >>
            emitInstr(KeyPart(id))
        }
      }
      
      case buckets.FixedSolution(_, solution, expr) =>
        emitMap(solution, expr, Eq) >> emitInstr(Extra)
      
      case buckets.Extra(expr) =>
        emitExpr(expr) >> emitInstr(Extra)
    }
    
    def emitExpr(expr: Expr): StateT[Id, Emission, Unit] = {
      emitLine(expr.loc.lineNum, expr.loc.line) >>
      (expr match {
        case ast.Let(loc, id, params, left, right) =>
          emitExpr(right)
        
        case ast.Import(_, _, child) =>
          emitExpr(child)

        case ast.New(loc, child) => 
          emitExpr(child) >> emitInstr(Map1(New))
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          emitExpr(in)
        
        case t @ ast.TicVar(loc, name) => { 
          t.binding match {
            case SolveBinding(solve) => {
              notImpl(expr)
              // TODO
              // emitOrDup(MarkTicVar(let, name)) {
                // StateT.apply[Id, Emission, Unit] { e =>
                  // e.ticVars((let, name))(e)     // assert: this will work iff lexical scoping is working
                // }
              // }
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
        
        case ast.ObjectDef(loc, props) => 
          /* def field2ObjInstr(t: (String, Expr)) = emitInstr(PushString(t._1)) >> emitExpr(t._2) >> emitInstr(Map2Cross(WrapObject))

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

          reduce(groups ++ joins) */
          notImpl(expr)

        case ast.ArrayDef(loc, values) => 
          /* val indexedValues = values.zipWithIndex

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

          joined >> fixedState */
          notImpl(expr)
        
        case ast.Descent(loc, child, property) => 
          // emitMapState(emitExpr(child), child.provenance, emitInstr(PushString(property)), ValueProvenance, DerefObject)
          notImpl(expr)
        
        case ast.Deref(loc, left, right) => 
          emitMap(left, right, DerefArray)
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case LoadBinding =>
              emitExpr(actuals.head) >> emitInstr(LoadLocal)

            case DistinctBinding =>
              emitExpr(actuals.head) >> emitInstr(Distinct)

            case Morphism1Binding(m) => 
              emitExpr(actuals.head) >> emitInstr(Morph1(BuiltInMorphism1(m)))

            case Morphism2Binding(m) => 
              emitExpr(actuals(0)) >> emitExpr(actuals(1)) >> emitInstr(Morph2(BuiltInMorphism2(m)))

            case ReductionBinding(f) =>
              emitExpr(actuals.head) >> emitInstr(Reduce(BuiltInReduction(f)))

            case Op1Binding(op) =>  
              emitUnary(actuals(0), BuiltInFunction1Op(op))

            case Op2Binding(op) =>
              emitMap(actuals(0), actuals(1), BuiltInFunction2Op(op))

            case LetBinding(let @ ast.Let(loc, id, params, left, right)) =>
              params.length match {
                case 0 =>
                  emitOrDup(MarkExpr(left))(emitExpr(left))

                case n => emitOrDup(MarkDispatch(let, actuals)) {
                  val actualStates = params zip actuals map {
                    case (name, expr) =>
                      labelTicVar(let, name)(emitExpr(expr))
                  }
                  
                  val body = if (actuals.length == n) {
                    emitExpr(left)
                  } else {
                    /* val spec = {
                      val init: BucketSpec = let.buckets.get         // assuming no errors
                      (params zip actuals).foldLeft(init) {
                        case (spec, (id, expr)) => spec.derive(id, expr)
                      }
                    }
                    
                    emitBucketSpec(let, spec) >> 
                      emitInstr(Split) >>
                      emitExpr(left) >>
                      emitInstr(Merge) */
                    notImpl(expr)
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

        case ast.Difference(loc, left, right) =>
          emitExpr(left) >> emitExpr(right) >> emitInstr(SetDifference) 

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
