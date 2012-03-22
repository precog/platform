package com.precog
package daze

import scala.collection.mutable

trait Memoizer extends DAG {
  import dag._
  
  val MemoThreshold = 1
  
  // TODO scale priority by cost
  def memoize(graph: DepGraph): DepGraph = {
    val reftable = countRefs(graph)
    val memotable = mutable.Map[DepGraph, DepGraph]()
    
    def memoize(graph: DepGraph): DepGraph = {
      def inner(graph: DepGraph): DepGraph = {
        lazy val refs = reftable.getOrElse(graph, 0)
        
        graph match {
          case _: SplitRoot => graph
          
          case _: Root => graph
          
          case New(loc, parent) => {
            if (refs > MemoThreshold)
              Memoize(New(loc, memoize(parent)), refs)
            else
              New(loc, memoize(parent))
          }
          
          case LoadLocal(loc, range, parent, tpe) => {
            if (refs > MemoThreshold)
              Memoize(LoadLocal(loc, range, memoize(parent), tpe), refs)
            else
              LoadLocal(loc, range, memoize(parent), tpe)
          }
          
          case Operate(loc, op, parent) => {
            if (refs > MemoThreshold)
              Memoize(Operate(loc, op, memoize(parent)), refs)
            else
              Operate(loc, op, memoize(parent))
          }
          
          case Reduce(loc, red, parent) => {
            if (refs > MemoThreshold)
              Memoize(Reduce(loc, red, memoize(parent)), refs)
            else
              Reduce(loc, red, memoize(parent))
          }          

          case SetReduce(loc, red, parent) => {
            if (refs > MemoThreshold)
              Memoize(SetReduce(loc, red, memoize(parent)), refs)
            else
              SetReduce(loc, red, memoize(parent))
          }
          
          case Split(loc, parent, child) => {
            val parent2 = memoize(parent)
            val child2 = memoize(child)
            
            if (refs > 1)
              Memoize(Split(loc, parent2, child2), refs)
            else
              Split(loc, parent2, child2)
          }
          
          case Join(loc, instr, left, right) => {
            val left2 = memoize(left)
            val right2 = memoize(right)
            
            if (refs > 1)
              Memoize(Join(loc, instr, left2, right2), refs)
            else
              Join(loc, instr, left2, right2)
          }
          
          case Filter(loc, cross, range, target, boolean) => {
            val target2 = memoize(target)
            val boolean2 = memoize(boolean)
            
            if (refs > 1)
              Memoize(Filter(loc, cross, range, target2, boolean2), refs)
            else
              Filter(loc, cross, range, target2, boolean2)
          }
          
          case Sort(parent, indexes) => {
            if (refs > MemoThreshold)
              Memoize(Sort(memoize(parent), indexes), refs)
            else
              Sort(memoize(parent), indexes)
          }
          
          case Memoize(parent, _) => memoize(parent)
        }
      }
      
      memotable get graph getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    memoize(graph)
  }
  
  def countRefs(graph: DepGraph): Map[DepGraph, Int] = graph match {
    case New(_, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case LoadLocal(_, _, parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case Operate(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case Reduce(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
        
    case SetReduce(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case Split(_, parent, child) =>
      merge(countRefs(child), increment(countRefs(parent), parent, 1))
    
    case Join(_, _, left, right) => {
      val rec = merge(countRefs(left), countRefs(right))
      increment(increment(rec, left, 1), right, 1)
    }
    
    case Filter(_, _, _, target, boolean) => {
      val rec = merge(countRefs(target), countRefs(boolean))
      increment(increment(rec, boolean, 1), target, 1)
    }
    
    case Sort(parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case Memoize(parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case _ => Map()
  }
  
  private def increment[A](map: Map[A, Int], a: A, delta: Int): Map[A, Int] = {
    val current = map get a getOrElse 0
    map.updated(a, current + delta)
  }
  
  private def merge[A](left: Map[A, Int], right: Map[A, Int]): Map[A, Int] = {
    left.foldLeft(right) {
      case (acc, (key, value)) => increment(acc, key, value)
    }
  }
}
