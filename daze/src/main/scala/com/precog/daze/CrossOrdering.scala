package com.precog
package daze

import scala.collection.mutable

trait CrossOrdering extends DAG {
  import instructions._
  import dag._

  def orderCrosses(node: DepGraph): DepGraph = {
    val memotable = mutable.Map[DepGraph, DepGraph]()
    
    def memoizedSpec(spec: BucketSpec, splits: => Map[dag.Split, dag.Split]): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
      
      case dag.Group(id, target, child) =>
        dag.Group(id, memoized(target, splits), memoizedSpec(child, splits))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, memoized(target, splits))
      
      case dag.Extra(target) =>
        dag.Extra(memoized(target, splits))
    }
    
    def memoized(node: DepGraph, _splits: => Map[dag.Split, dag.Split]): DepGraph = {
      lazy val splits = _splits
      
      def inner(node: DepGraph): DepGraph = node match {
        case node @ SplitParam(loc, index) => SplitParam(loc, index)(splits(node.parent))
        
        case node @ SplitGroup(loc, index, provenance) => SplitGroup(loc, index, provenance)(splits(node.parent))
        
        case node @ Root(_, _) => node
        
        case dag.New(loc, parent) =>
          dag.New(loc, memoized(parent, splits))
        
        case dag.LoadLocal(loc, range, parent, tpe) =>
          dag.LoadLocal(loc, range, memoized(parent, splits), tpe)
        
        case Operate(loc, op, parent) =>
          Operate(loc, op, memoized(parent, splits))
        
        case dag.Morph1(loc, m, parent) =>
          dag.Morph1(loc, m, memoized(parent, splits))
        
        case dag.Morph2(loc, m, left, right) =>
          dag.Morph2(loc, m, memoized(left, splits), memoized(right, splits))
        
        case dag.Distinct(loc, parent) =>
          dag.Distinct(loc, memoized(parent, splits))
                
        case dag.Reduce(loc, red, parent) =>
          dag.Reduce(loc, red, memoized(parent, splits))
        
        case s @ dag.Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> result)
          lazy val spec2 = memoizedSpec(spec, splits2)
          lazy val child2 = memoized(child, splits2)
          lazy val result: dag.Split = dag.Split(loc, spec2, child2)
          result
        }
        
        case Join(loc, instr: Map2Match, left, right) => {
          val left2 = memoized(left, splits)
          val right2 = memoized(right, splits)
          
          val (leftIndexes, rightIndexes) = determineSort(left2, right2)
          
          val leftPrefix = leftIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          val rightPrefix = rightIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          
          if (leftPrefix && rightPrefix)
            Join(loc, instr, left2, right2)
          else if (leftPrefix && !rightPrefix)
            Join(loc, instr, left2, Sort(right2, rightIndexes))
          else if (!leftPrefix && rightPrefix)
            Join(loc, instr, Sort(left2, leftIndexes), right2)
          else
            Join(loc, instr, Sort(left2, leftIndexes), Sort(right2, rightIndexes))
        }
        
        case Join(loc, Map2Cross(op), left, right) => {
          if (right.isSingleton)
            Join(loc, Map2CrossLeft(op), memoized(left, splits), memoized(right, splits))
          else if (left.isSingleton)
            Join(loc, Map2CrossRight(op), memoized(left, splits), memoized(right, splits))
          else
            Join(loc, Map2CrossLeft(op), memoized(left, splits), Memoize(memoized(right, splits), 100))
        }
        
        case Join(loc, instr, left, right) =>
          Join(loc, instr, memoized(left, splits), memoized(right, splits))
        
        case Filter(loc, None, target, boolean) => {
          val target2 = memoized(target, splits)
          val boolean2 = memoized(boolean, splits)
          
          val (targetIndexes, booleanIndexes) = determineSort(target2, boolean2)
          
          val targetPrefix = targetIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          val booleanPrefix = booleanIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          
          if (targetPrefix && booleanPrefix)
            Filter(loc, None, target2, boolean2)
          else if (targetPrefix && !booleanPrefix)
            Filter(loc, None, target2, Sort(boolean2, booleanIndexes))
          else if (!targetPrefix && booleanPrefix)
            Filter(loc, None, Sort(target2, targetIndexes), boolean2)
          else  
            Filter(loc, None, Sort(target2, targetIndexes), Sort(boolean2, booleanIndexes))
        }
        
        case Filter(loc, cross, target, boolean) =>
          Filter(loc, cross, memoized(target, splits), memoized(boolean, splits))
        
        case Sort(parent, _) => memoized(parent, splits)
        
        case Memoize(parent, priority) => Memoize(memoized(parent, splits), priority)
      }
  
      memotable.get(node) getOrElse {
        val result = inner(node)
        memotable += (node -> result)
        result
      }
    }
    
    memoized(node, Map())
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
