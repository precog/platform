package com.precog
package daze

import java.util.concurrent.ConcurrentHashMap

trait CrossOrdering extends DAG {
  import instructions._
  import dag._

  private[this] val memotable = new ConcurrentHashMap[DepGraph, DepGraph]
  
  def orderCrosses(node: DepGraph): DepGraph = {
    def inner(node: DepGraph): DepGraph = node match {
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

    Option(memotable.get(node)) getOrElse {
      val result = inner(node)
      val result2 = memotable.putIfAbsent(node, result)

      if (result2 == null)
        result
      else
        result2
    }
  }

  private def determineSort(left2: DepGraph, right2: DepGraph): (Vector[Int], Vector[Int]) = {
    val leftPairs = left2.provenance.zipWithIndex filter {
      case (p, i) => right2.provenance contains p
    }
    
    val rightPairs = right2.provenance.zipWithIndex filter {
      case (p, i) => left2.provenance contains p
    }
    
    val (_, leftIndexes) = leftPairs.unzip
    
    val (_, rightIndexes) = rightPairs sortWith {
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
    } unzip

    (leftIndexes, rightIndexes)
  }
}
