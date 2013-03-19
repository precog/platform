package com.precog
package daze

import scala.collection.mutable

import com.precog.util.IdGen

trait DAGTransform extends DAG {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def transformBottomUp(graph: DepGraph, splits: Set[dag.Split])(f : DepGraph => DepGraph) : DepGraph = {

    val memotable = mutable.Map[DepGraph, DepGraph]()

    def transformSpec(splits: => Map[Split, Split], spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(transformSpec(splits, left), transformSpec(splits, right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(transformSpec(splits, left), transformSpec(splits, right))
      
      case Group(id, target, child) =>
        Group(id, transformAux(splits, target), transformSpec(splits, child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, transformAux(splits, target))
      
      case Extra(target) =>
        Extra(transformAux(splits, target))
    }

    def transformAux(splits0: => Map[Split, Split], graph: DepGraph) : DepGraph = {
      lazy val splits = splits0
      
      def inner(graph: DepGraph): DepGraph = graph match {
        case r : Root => f(r)
  
        case graph @ New(parent) => f(New(transformAux(splits, parent))(graph.loc))
        
        case graph @ LoadLocal(parent, jtpe) => f(LoadLocal(transformAux(splits, parent), jtpe)(graph.loc))
  
        case graph @ Operate(op, parent) => f(Operate(op, transformAux(splits, parent))(graph.loc))
  
        case graph @ Reduce(red, parent) => f(Reduce(red, transformAux(splits, parent))(graph.loc))
        
        case MegaReduce(reds, parent) => f(MegaReduce(reds, transformAux(splits, parent)))
  
        case graph @ Morph1(m, parent) => f(Morph1(m, transformAux(splits, parent))(graph.loc))
  
        case graph @ Morph2(m, left, right) => f(Morph2(m, transformAux(splits, left), transformAux(splits, right))(graph.loc))
  
        case graph @ Join(op, joinSort, left, right) => f(Join(op, joinSort, transformAux(splits, left), transformAux(splits, right))(graph.loc))
  
        case graph @ Assert(pred, child) => f(Assert(transformAux(splits, pred), transformAux(splits, child))(graph.loc))
        
        case graph @ Cond(pred, left, leftJoin, right, rightJoin) => f(Cond(transformAux(splits, pred), transformAux(splits, left), leftJoin, transformAux(splits, right), rightJoin)(graph.loc))
        
        case graph @ Observe(data, samples) => f(Observe(transformAux(splits, data), transformAux(splits, samples))(graph.loc))
        
        case graph @ IUI(union, left, right) => f(IUI(union, transformAux(splits, left), transformAux(splits, right))(graph.loc))

        case graph @ Diff(left, right) => f(Diff(transformAux(splits, left), transformAux(splits, right))(graph.loc))

        case graph @ Filter(cross, target, boolean) =>
          f(Filter(cross, transformAux(splits, target), transformAux(splits, boolean))(graph.loc))
  
        case Sort(parent, indices) => f(Sort(transformAux(splits, parent), indices))

        case SortBy(parent, sortField, valueField, id) => f(SortBy(transformAux(splits, parent), sortField, valueField, id))
        
        case ReSortBy(parent, id) => f(ReSortBy(transformAux(splits, parent), id))
  
        case Memoize(parent, priority) => f(Memoize(transformAux(splits, parent), priority))
  
        case graph @ Distinct(parent) => f(Distinct(transformAux(splits, parent))(graph.loc))
  
        case s @ Split(spec, child) => {
          lazy val splits2 = splits + (s -> s2)
          lazy val spec2 = transformSpec(splits2, spec)
          lazy val child2 = transformAux(splits2, child)
          lazy val s2: Split = Split(spec2, child2)(s.loc)
          f(s2)
        }
  
        // not using extractors due to bug
        case s: SplitGroup =>
          f(SplitGroup(s.id, s.identities)(splits(s.parent))(s.loc))
  
        // not using extractors due to bug
        case s: SplitParam =>
          f(SplitParam(s.id)(splits(s.parent))(s.loc))
      }

      memotable.get(graph) getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    transformAux(splits.zip(splits)(collection.breakOut), graph)
  }
}
