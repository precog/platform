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
    
    def memoizedSpec(spec: BucketSpec, splits: => Map[Split, Split]): BucketSpec = spec match {
      case MergeBucketSpec(left, right, and) =>
        MergeBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits), and)
      
      case ZipBucketSpec(left, right) =>
        ZipBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
      
      case SingleBucketSpec(target, solution) =>
        SingleBucketSpec(memoized(target, splits), memoized(solution, splits))
    }
    
    def memoized(graph: DepGraph, _splits: => Map[Split, Split]): DepGraph = {
      lazy val splits = _splits
      
      def inner(graph: DepGraph): DepGraph = {
        lazy val refs = reftable.getOrElse(graph, 0)
        
        graph match {
          case s @ SplitParam(loc, index) => SplitParam(loc, index)(splits(s.parent))
          
          case s @ SplitGroup(loc, index, provenance) => SplitGroup(loc, index, provenance)(splits(s.parent))
          
          case _: Root => graph
          
          case New(loc, parent) => {
            if (refs > MemoThreshold)
              Memoize(New(loc, memoized(parent, splits)), refs)
            else
              New(loc, memoized(parent, splits))
          }
          
          case LoadLocal(loc, range, parent, tpe) => {
            if (refs > MemoThreshold)
              Memoize(LoadLocal(loc, range, memoized(parent, splits), tpe), refs)
            else
              LoadLocal(loc, range, memoized(parent, splits), tpe)
          }
          
          case Operate(loc, op, parent) => {
            if (refs > MemoThreshold)
              Memoize(Operate(loc, op, memoized(parent, splits)), refs)
            else
              Operate(loc, op, memoized(parent, splits))
          }
          
          case Reduce(loc, red, parent) => {
            if (refs > MemoThreshold)
              Memoize(Reduce(loc, red, memoized(parent, splits)), refs)
            else
              Reduce(loc, red, memoized(parent, splits))
          }          

          case SetReduce(loc, red, parent) => {
            if (refs > MemoThreshold)
              Memoize(SetReduce(loc, red, memoized(parent, splits)), refs)
            else
              SetReduce(loc, red, memoized(parent, splits))
          }
          
          case s @ Split(loc, specs, child) => {
            lazy val splits2 = splits + (s -> result)
            lazy val specs2 = specs map { s => memoizedSpec(s, splits2) }
            lazy val child2 = memoized(child, splits2)
            lazy val result: Split = Split(loc, specs2, child2)
            result
          }
          
          case Join(loc, instr, left, right) => {
            val left2 = memoized(left, splits)
            val right2 = memoized(right, splits)
            
            if (refs > 1)
              Memoize(Join(loc, instr, left2, right2), refs)
            else
              Join(loc, instr, left2, right2)
          }
          
          case Filter(loc, cross, range, target, boolean) => {
            val target2 = memoized(target, splits)
            val boolean2 = memoized(boolean, splits)
            
            if (refs > 1)
              Memoize(Filter(loc, cross, range, target2, boolean2), refs)
            else
              Filter(loc, cross, range, target2, boolean2)
          }
          
          case Sort(parent, indexes) => {
            if (refs > MemoThreshold)
              Memoize(Sort(memoized(parent, splits), indexes), refs)
            else
              Sort(memoized(parent, splits), indexes)
          }
          
          case Memoize(parent, _) => memoized(parent, splits)
        }
      }
      
      memotable get graph getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    memoized(graph, Map())
  }
  
  def countSpecRefs(spec: BucketSpec): Map[DepGraph, Int] = spec match {
    case MergeBucketSpec(left, right, _) =>
      merge(countSpecRefs(left), countSpecRefs(right))
    
    case ZipBucketSpec(left, right) =>
      merge(countSpecRefs(left), countSpecRefs(right))
    
    case SingleBucketSpec(target, solution) =>
      merge(increment(countRefs(target), target, 1), increment(countRefs(solution), solution, 1))
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
    
    case Split(_, specs, child) =>
      merge(countRefs(child), specs map countSpecRefs reduce merge[DepGraph])
    
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
