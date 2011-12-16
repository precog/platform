package com.querio.bytecode

trait DAG extends Instructions {
  import instructions._
  
  sealed trait DepGraph {
    val loc: Line
  }
  
  object dag {
    case class Root(loc: Line, instr: RootInstr) extends DepGraph
    
    case class LoadLocal(loc: Line, ranges: Set[IndexRange], parent: DepGraph) extends DepGraph
    
    case class Operate(loc: Line, instr: OpInstr, parent: DepGraph) extends DepGraph
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph
    
    case class Split(loc: Line, parent: DepGraph) extends DepGraph
    case class Merge(loc: Line, parent: DepGraph) extends DepGraph
    
    case class Join(loc: Line, instr: JoinInstr, left: DepGraph, right: DepGraph) extends DepGraph
    case class Filter(loc: Line, cross: Boolean, ranges: Set[IndexRange], target: DepGraph, boolean: DepGraph) extends DepGraph
    
    
    sealed trait IndexRange
    
    case class Conjunction(left: IndexRange, right: IndexRange) extends IndexRange
    case class Disjunction(left: IndexRange, right: IndexRange) extends IndexRange
    case class Complementation(child: IndexRange) extends IndexRange
    
    // [start, end)
    case class Contiguous(start: RangeOperand, end: RangeOperand) extends IndexRange
    
    
    sealed trait RangeOperand
    
    case class ValueOperand(source: DepGraph) extends RangeOperand
    case class PropertyOperand(source: DepGraph) extends RangeOperand
    case class IndexOperand(source: DepGraph) extends RangeOperand
    
    case class BinaryOperand(left: RangeOperand, op: PredicateOp with BinaryOperation, right: RangeOperand) extends RangeOperand
    case class UnaryOperand(op: PredicateOp with UnaryOperation, child: RangeOperand) extends RangeOperand
  }
}
