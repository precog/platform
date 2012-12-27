package com.precog
package daze

import scala.collection.mutable

import scalaz.std.set._
import scalaz.std.map._
import scalaz.syntax.monoid._

trait Memoizer extends DAG {
  import dag._
  
  val MemoThreshold = 1
  
  def memoize(graph: DepGraph): DepGraph = {
    val refs = findForcingRefs(graph, OpSide.Center(graph))
    
    def numRefs(node: DepGraph) = refs get node map { _.size } getOrElse 0
    
    applyMemoizations(graph, refs)
  }
  
  def scaleMemoPriority(count: Int): Int = count    // TODO exponential function?
  
  private def applyMemoizations(target: DepGraph, refs: Map[DepGraph, Set[OpSide]]): DepGraph = {
    import OpSide._
    
    val memotable = mutable.Map[DepGraph, DepGraph]()
    
    def numRefs(node: DepGraph) = refs get node map { _.size } getOrElse 0
    
    def memoized(_splits: => Map[dag.Split, dag.Split])(node: DepGraph): DepGraph = {
      lazy val splits = _splits
      
      def inner(target: DepGraph): DepGraph = target match {
        case s @ dag.SplitParam(loc, id) => dag.SplitParam(loc, id)(splits(s.parent))
        
        case s @ dag.SplitGroup(loc, id, identities) => dag.SplitGroup(loc, id, identities)(splits(s.parent))
        
        case dag.Const(_, _) => target

        case dag.Undefined(_) => target
        
        case dag.New(loc, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.New(loc, memoized(splits)(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.New(loc, memoized(splits)(parent))
        }
        
        case node @ dag.Morph1(loc, m, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Morph1(loc, m, memoized(splits)(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.Morph1(loc, m, memoized(splits)(parent))
        }
        
        case node @ dag.Morph2(loc, m, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Morph2(loc, m, memoized(splits)(left), memoized(splits)(right)), scaleMemoPriority(numRefs(node)))
          else
            dag.Morph2(loc, m, memoized(splits)(left), memoized(splits)(right))
        }
        
        case node @ dag.Distinct(loc, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Distinct(loc, memoized(splits)(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.Distinct(loc, memoized(splits)(parent))
        }
        
        case dag.LoadLocal(loc, parent, jtpe) =>
          dag.LoadLocal(loc, memoized(splits)(parent), jtpe)
        
        case dag.Operate(loc, op, parent) =>
          dag.Operate(loc, op, memoized(splits)(parent))
        
        case node @ dag.Reduce(loc, red, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Reduce(loc, red, memoized(splits)(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.Reduce(loc, red, memoized(splits)(parent))
        }
        
        case node @ dag.MegaReduce(loc, reds, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.MegaReduce(loc, reds, memoized(splits)(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.MegaReduce(loc, reds, memoized(splits)(parent))
        }
        
        case s @ dag.Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> result)
          lazy val spec2 = memoizedSpec(spec, splits2)
          lazy val child2 = memoized(splits2)(child)
          lazy val result: dag.Split = dag.Split(loc, spec2, child2)
          
          if (numRefs(s) > MemoThreshold)
            Memoize(result, scaleMemoPriority(numRefs(s)))
          else
            result
        }
        
        case node @ dag.IUI(loc, union, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.IUI(loc, union, memoized(splits)(left), memoized(splits)(right)), scaleMemoPriority(numRefs(node)))
          else
            dag.IUI(loc, union, memoized(splits)(left), memoized(splits)(right))
        }
        
        case node @ dag.Diff(loc, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Diff(loc, memoized(splits)(left), memoized(splits)(right)), scaleMemoPriority(numRefs(node)))
          else
            dag.Diff(loc, memoized(splits)(left), memoized(splits)(right))
        }
        
        case node @ dag.Join(loc, op, joinSort, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Join(loc, op, joinSort, memoized(splits)(left), memoized(splits)(right)), scaleMemoPriority(numRefs(node)))
          else
            dag.Join(loc, op, joinSort, memoized(splits)(left), memoized(splits)(right))
        }
        
        case node @ dag.Filter(loc, joinSort, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Filter(loc, joinSort, memoized(splits)(left), memoized(splits)(right)), scaleMemoPriority(numRefs(node)))
          else
            dag.Filter(loc, joinSort, memoized(splits)(left), memoized(splits)(right))
        }

        case dag.Sort(parent, indexes) => dag.Sort(memoized(splits)(parent), indexes)

        case dag.SortBy(parent, sortField, valueField, id) => dag.SortBy(memoized(splits)(parent), sortField, valueField, id)

        case dag.ReSortBy(parent, id) => dag.ReSortBy(memoized(splits)(parent), id)

        case dag.Memoize(parent, priority) => dag.Memoize(memoized(splits)(parent), priority)
      }

      def memoizedSpec(spec: dag.BucketSpec, splits: => Map[dag.Split, dag.Split]): dag.BucketSpec = spec match {  //TODO generalize?
        case dag.UnionBucketSpec(left, right) =>
          dag.UnionBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
        
        case dag.IntersectBucketSpec(left, right) =>
          dag.IntersectBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
        
        case dag.Group(id, target, child) =>
          dag.Group(id, memoized(splits)(target), memoizedSpec(child, splits))
        
        case dag.UnfixedSolution(id, target) =>
          dag.UnfixedSolution(id, memoized(splits)(target))
        
        case dag.Extra(target) =>
          dag.Extra(memoized(splits)(target))
      }

      memotable.get(node) getOrElse {
        val result = inner(node)
        memotable += (node -> result)
        result
      }
    }
    
    memoized(Map())(target)
  }
  
  private def findForcingRefsInSpec(spec: BucketSpec, split: Split): Map[DepGraph, Set[OpSide]] = spec match {
    case UnionBucketSpec(left, right) =>
      findForcingRefsInSpec(left, split) |+| findForcingRefsInSpec(right, split)
    
    case IntersectBucketSpec(left, right) =>
      findForcingRefsInSpec(left, split) |+| findForcingRefsInSpec(right, split)
    
    case Group(id, target, forest) =>
      findForcingRefs(target, OpSide.Center(split)) |+| findForcingRefsInSpec(forest, split)
    
    case UnfixedSolution(_, solution) =>
      findForcingRefs(solution, OpSide.Center(split))
    
    case Extra(expr) =>
      findForcingRefs(expr, OpSide.Center(split))
  }
  
  private def findForcingRefs(graph: DepGraph, force: OpSide): Map[DepGraph, Set[OpSide]] = graph match {
    case SplitParam(_, _) | SplitGroup(_, _, _) | Const(_, _) | Undefined(_) => Map()
    
    case New(_, parent) =>
      updateMap(findForcingRefs(parent, force), graph, force)
    
    case Morph1(_, _, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)
    
    case Morph2(_, _, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Distinct(_, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)
    
    case LoadLocal(_, parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph))      // load is a forcing point, but not a memo candidate
    
    case Operate(_, _, parent) =>
      findForcingRefs(parent, force)
    
    case Reduce(_, _, parent) =>
      findForcingRefs(parent, OpSide.Center(graph))      // reduce is a forcing point, but not a memo candidate
    
    case MegaReduce(_, _, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)
    
    case graph @ Split(_, spec, child) => {
      val childRefs = findForcingRefs(child, OpSide.Center(graph))    // TODO is this right?
      val specRefs = findForcingRefsInSpec(spec, graph)
      
      updateMap(childRefs |+| specRefs, graph, force)
    }
    
    case IUI(_, _, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Diff(_, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Join(_, _, CrossLeftSort | CrossRightSort, left, right) if !left.isInstanceOf[Root] && !right.isInstanceOf[Root] => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    // an approximation of table heritage that *should* be accurate
    case Join(_, _, IdentitySort | ValueSort(_), left, right) if left.identities != right.identities => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Join(_, _, _, left, right) =>
      findForcingRefs(left, force) |+| findForcingRefs(right, force)
    
    case Filter(_, CrossLeftSort | CrossRightSort, target, boolean) if !target.isInstanceOf[Root] && !boolean.isInstanceOf[Root] => {
      val merged = findForcingRefs(target, OpSide.Left(graph)) |+| findForcingRefs(boolean, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    // an approximation of table heritage that *should* be accurate
    case Filter(_, IdentitySort | ValueSort(_), target, boolean) if target.identities != boolean.identities => {
      val merged = findForcingRefs(target, OpSide.Left(graph)) |+| findForcingRefs(boolean, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Filter(_, _, target, boolean) =>
      findForcingRefs(target, force) |+| findForcingRefs(boolean, force)
    
    case Sort(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph))      // sort is a forcing point, but not a memo candidate
    
    case SortBy(parent, _, _, _) =>
      findForcingRefs(parent, OpSide.Center(graph))      // sort is a forcing point, but not a memo candidate
    
    case ReSortBy(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph))      // sort is a forcing point, but not a memo candidate
    
    case Memoize(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph))      // memoize is a forcing point, but not a memo candidate
  }
  
  private def updateMap(refs: Map[DepGraph, Set[OpSide]], graph: DepGraph, force: OpSide): Map[DepGraph, Set[OpSide]] = {
    val set = refs get graph getOrElse Set()
    refs + (graph -> (set + force))
  }
  
  private sealed trait OpSide
  
  private object OpSide {
    case class Left(graph: DepGraph) extends OpSide
    case class Right(graph: DepGraph) extends OpSide
    case class Center(graph: DepGraph) extends OpSide
  }
}
