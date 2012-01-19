package com.querio
package daze

trait CrossOrdering extends DAG {
  import instructions._
  import dag._
  
  // TODO memoize in future
  def orderCrosses(node: DepGraph): DepGraph = node match {
    case node @ SplitRoot(_, _) => node
    
    case node @ Root(_, _) => node
    
    case dag.New(loc, parent) =>
      dag.New(loc, orderCrosses(parent))
    
    case dag.LoadLocal(loc, range, parent, tpe) =>
      dag.LoadLocal(loc, range, orderCrosses(parent), tpe)
    
    case Operate(loc, op, parent) =>
      Operate(loc, op, orderCrosses(parent))
    
    case dag.Reduce(loc, red, parent) =>
      dag.Reduce(loc, red, orderCrosses(parent))
    
    case dag.Split(loc, parent, child) =>
      dag.Split(loc, orderCrosses(parent), orderCrosses(child))
    
    case Join(loc, instr: Map2Match, left, right) => {
      val left2 = orderCrosses(left)
      val right2 = orderCrosses(right)
      
      val (leftIndexes, rightIndexes) = determineSort(left2, right2)
      
      Join(loc, instr, Sort(left2, leftIndexes), Sort(right2, rightIndexes))
    }
    
    case Join(loc, instr, left, right) =>
      Join(loc, instr, orderCrosses(left), orderCrosses(right))
    
    case Filter(loc, None, range, target, boolean) => {
      val target2 = orderCrosses(target)
      val boolean2 = orderCrosses(boolean)
      
      val (targetIndexes, booleanIndexes) = determineSort(target2, boolean2)
      
      Filter(loc, None, range, Sort(target2, targetIndexes), Sort(boolean2, booleanIndexes))
    }
    
    case Filter(loc, cross, range, target, boolean) =>
      Filter(loc, cross, range, orderCrosses(target), orderCrosses(boolean))
    
    case Sort(parent, _) => orderCrosses(parent)
  }

  private def determineSort(left2: DepGraph, right2: DepGraph): (Vector[Int], Vector[Int]) = {
    val leftPairs = left2.provenance.zipWithIndex collect {
      case (p, i) if right2.provenance contains p => (p, i)
    }
    
    val rightPairs = right2.provenance.zipWithIndex collect {
      case (p, i) if left2.provenance contains p => (p, i)
    }
    
    val leftIndexes = leftPairs map { case (_, i) => i }
    
    val rightIndexes = rightPairs sortWith {
      case ((p1, i1), (p2, i2)) => {
        val leftIndex = leftPairs indexWhere {
          case (`p1`, _) => true
          case _ => false
        }
        
        val rightIndex = leftPairs indexWhere {
          case (`p2`, _) => true
          case _ => false
        }
        
        leftIndex < rightIndex
      }
    } map {
      case (_, i) => i
    }
    
    (leftIndexes, rightIndexes)
  }
}
