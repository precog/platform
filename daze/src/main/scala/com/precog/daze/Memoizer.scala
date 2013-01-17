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
        // not using extractors due to bug
        case s: dag.SplitParam =>
          dag.SplitParam(s.id)(splits(s.parent))(s.loc)
        
        // not using extractors due to bug
        case s: dag.SplitGroup =>
          dag.SplitGroup(s.id, s.identities)(splits(s.parent))(s.loc)
        
        case dag.Const(_) => target

        case dag.Undefined() => target
        
        case target @ dag.New(parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.New(memoized(splits)(parent))(target.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.New(memoized(splits)(parent))(target.loc)
        }
        
        case node @ dag.Morph1(m, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Morph1(m, memoized(splits)(parent))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Morph1(m, memoized(splits)(parent))(node.loc)
        }
        
        case node @ dag.Morph2(m, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Morph2(m, memoized(splits)(left), memoized(splits)(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Morph2(m, memoized(splits)(left), memoized(splits)(right))(node.loc)
        }
        
        case node @ dag.Distinct(parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Distinct(memoized(splits)(parent))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Distinct(memoized(splits)(parent))(node.loc)
        }
        
        case target @ dag.LoadLocal(parent, jtpe) =>
          dag.LoadLocal(memoized(splits)(parent), jtpe)(target.loc)
        
        case target @ dag.Operate(op, parent) =>
          dag.Operate(op, memoized(splits)(parent))(target.loc)
        
        case node @ dag.Reduce(red, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Reduce(red, memoized(splits)(parent))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Reduce(red, memoized(splits)(parent))(node.loc)
        }
        
        case node @ dag.MegaReduce(reds, parent) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.MegaReduce(reds, memoized(splits)(parent)), scaleMemoPriority(numRefs(node)))
          else
            dag.MegaReduce(reds, memoized(splits)(parent))
        }
        
        case s @ dag.Split(spec, child) => {
          lazy val splits2 = splits + (s -> result)
          lazy val spec2 = memoizedSpec(spec, splits2)
          lazy val child2 = memoized(splits2)(child)
          lazy val result: dag.Split = dag.Split(spec2, child2)(s.loc)
          
          if (numRefs(s) > MemoThreshold)
            Memoize(result, scaleMemoPriority(numRefs(s)))
          else
            result
        }
        
        case node @ dag.Assert(pred, child) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Assert(memoized(splits)(pred), memoized(splits)(child))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Assert(memoized(splits)(pred), memoized(splits)(child))(node.loc)
        }
        
        case node @ dag.IUI(union, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.IUI(union, memoized(splits)(left), memoized(splits)(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.IUI(union, memoized(splits)(left), memoized(splits)(right))(node.loc)
        }
        
        case node @ dag.Diff(left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Diff(memoized(splits)(left), memoized(splits)(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Diff(memoized(splits)(left), memoized(splits)(right))(node.loc)
        }
        
        case node @ dag.Join(op, joinSort, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Join(op, joinSort, memoized(splits)(left), memoized(splits)(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Join(op, joinSort, memoized(splits)(left), memoized(splits)(right))(node.loc)
        }
        
        case node @ dag.Filter(joinSort, left, right) => {
          if (numRefs(node) > MemoThreshold)
            Memoize(dag.Filter(joinSort, memoized(splits)(left), memoized(splits)(right))(node.loc), scaleMemoPriority(numRefs(node)))
          else
            dag.Filter(joinSort, memoized(splits)(left), memoized(splits)(right))(node.loc)
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
    case _: SplitParam | _: SplitGroup | Const(_) | Undefined() => Map()
    
    case New(parent) =>
      updateMap(findForcingRefs(parent, force), graph, force)
    
    case Morph1(_, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)
    
    case Morph2(_, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Distinct(parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)
    
    case LoadLocal(parent, _) =>
      findForcingRefs(parent, OpSide.Center(graph))      // load is a forcing point, but not a memo candidate
    
    case Operate(_, parent) =>
      findForcingRefs(parent, force)
    
    case Reduce(_, parent) =>
      findForcingRefs(parent, OpSide.Center(graph))      // reduce is a forcing point, but not a memo candidate
    
    case MegaReduce(_, parent) =>
      updateMap(findForcingRefs(parent, OpSide.Center(graph)), graph, force)
    
    case graph @ Split(spec, child) => {
      val childRefs = findForcingRefs(child, OpSide.Center(graph))    // TODO is this right?
      val specRefs = findForcingRefsInSpec(spec, graph)
      
      updateMap(childRefs |+| specRefs, graph, force)
    }
    
    case Assert(pred, child) => {
      val merged = findForcingRefs(pred, OpSide.Left(graph)) |+| findForcingRefs(child, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case IUI(_, left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Diff(left, right) => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Join(_, CrossLeftSort | CrossRightSort, left, right) if !left.isInstanceOf[Root] && !right.isInstanceOf[Root] => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    // an approximation of table heritage that *should* be accurate
    case Join(_, IdentitySort | ValueSort(_), left, right) if left.identities != right.identities => {
      val merged = findForcingRefs(left, OpSide.Left(graph)) |+| findForcingRefs(right, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Join(_, _, left, right) =>
      findForcingRefs(left, force) |+| findForcingRefs(right, force)
    
    case Filter(CrossLeftSort | CrossRightSort, target, boolean) if !target.isInstanceOf[Root] && !boolean.isInstanceOf[Root] => {
      val merged = findForcingRefs(target, OpSide.Left(graph)) |+| findForcingRefs(boolean, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    // an approximation of table heritage that *should* be accurate
    case Filter(IdentitySort | ValueSort(_), target, boolean) if target.identities != boolean.identities => {
      val merged = findForcingRefs(target, OpSide.Left(graph)) |+| findForcingRefs(boolean, OpSide.Right(graph))
      updateMap(merged, graph, force)
    }
    
    case Filter(_, target, boolean) =>
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
