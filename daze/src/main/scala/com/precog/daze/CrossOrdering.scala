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
        // not using extractors due to bug
        case node: SplitParam =>
          SplitParam(node.id)(splits(node.parent))(node.loc)
        
        // not using extractors due to bug
        case node: SplitGroup =>
          SplitGroup(node.id, node.identities)(splits(node.parent))(node.loc)
        
        case node @ Const(_) => node

        case node @ Undefined() => node
        
        case node @ dag.New(parent) =>
          dag.New(memoized(parent, splits))(node.loc)
        
        case node @ dag.LoadLocal(parent, tpe) =>
          dag.LoadLocal(memoized(parent, splits), tpe)(node.loc)

        case node @ Operate(op, parent) =>
          Operate(op, memoized(parent, splits))(node.loc)
        
        case node @ dag.Morph1(m, parent) =>
          dag.Morph1(m, memoized(parent, splits))(node.loc)
        
        case node @ dag.Morph2(m, left, right) =>
          dag.Morph2(m, memoized(left, splits), memoized(right, splits))(node.loc)
        
        case node @ dag.Distinct(parent) =>
          dag.Distinct(memoized(parent, splits))(node.loc)
                
        case node @ dag.Reduce(red, parent) =>
          dag.Reduce(red, memoized(parent, splits))(node.loc)
        
        case dag.MegaReduce(reds, parent) =>
          dag.MegaReduce(reds, memoized(parent, splits))

        case s @ dag.Split(spec, child) => {
          lazy val splits2 = splits + (s -> result)
          lazy val spec2 = memoizedSpec(spec, splits2)
          lazy val child2 = memoized(child, splits2)
          lazy val result: dag.Split = dag.Split(spec2, child2)(s.loc)
          result
        }
        
        case node @ dag.Assert(pred, child) =>
          dag.Assert(memoized(pred, splits), memoized(child, splits))(node.loc)
        
        case node @ dag.Observe(data, samples) =>
          dag.Observe(memoized(data, splits), memoized(samples, splits))(node.loc)
        
        case node @ IUI(union, left, right) =>
          IUI(union, memoized(left, splits), memoized(right, splits))(node.loc)
        
        case node @ Diff(left, right) =>
          Diff(memoized(left, splits), memoized(right, splits))(node.loc)
        
        case node @ Join(op, ValueSort(id), left, right) => {
          val left2 = memoized(left, splits)
          val right2 = memoized(right, splits)
          
          def resortLeft = ReSortBy(left2, id)
          def resortRight = ReSortBy(right2, id)
          
          (left2.sorting, right2.sorting) match {
            case (ValueSort(`id`), ValueSort(`id`)) => Join(op, ValueSort(id), left2, right2)(node.loc)
            case (ValueSort(`id`), _              ) => Join(op, ValueSort(id), left2, resortRight)(node.loc)
            case (_,               ValueSort(`id`)) => Join(op, ValueSort(id), resortLeft, right2)(node.loc)
            case _                                  => Join(op, ValueSort(id), resortLeft, resortRight)(node.loc)
          }
        }

        case node @ Join(op, IdentitySort, left, right) => {
          val left2 = memoized(left, splits)
          val right2 = memoized(right, splits)
          
          val (leftIndices, rightIndices) = determineSort(left2, right2)
          
          val leftPrefix = leftIndices zip (Stream from 0) forall { case (a, b) => a == b }
          val rightPrefix = rightIndices zip (Stream from 0) forall { case (a, b) => a == b }
          
          def sortLeft = Sort(left2, leftIndices)
          def sortRight = Sort(right2, rightIndices)
          
          def sortLeftAux = Sort(left2, Vector(0 until left2.identities.length: _*))
          def sortRightAux = Sort(right2, Vector(0 until right2.identities.length: _*))
          
          (left2.sorting, leftPrefix, right2.sorting, rightPrefix) match {
            case (IdentitySort, true,  IdentitySort, true ) => Join(op, IdentitySort, left2, right2)(node.loc)
            case (IdentitySort, true,  IdentitySort, false) => Join(op, IdentitySort, left2, sortRight)(node.loc)
            case (IdentitySort, false, IdentitySort, true ) => Join(op, IdentitySort, sortLeft, right2)(node.loc)
            case (IdentitySort, false, IdentitySort, false) => Join(op, IdentitySort, sortLeft, sortRight)(node.loc)
            
            case (_,            _,     IdentitySort, true ) => Join(op, IdentitySort, sortLeftAux, right2)(node.loc)
            case (_,            _,     IdentitySort, false) => Join(op, IdentitySort, sortLeftAux, sortRight)(node.loc)
            
            case (IdentitySort, true,  _,            _    ) => Join(op, IdentitySort, left2, sortRightAux)(node.loc)
            case (IdentitySort, false, _,            _    ) => Join(op, IdentitySort, sortLeft, sortRightAux)(node.loc)
            
            case _                                          => Join(op, IdentitySort, sortLeftAux, sortRightAux)(node.loc)
              
          }
        }
        
        case node @ Join(op, CrossLeftSort | CrossRightSort, left, right) => {
          if (right.isSingleton)
            Join(op, CrossLeftSort, memoized(left, splits), memoized(right, splits))(node.loc)
          else if (left.isSingleton)
            Join(op, CrossRightSort, memoized(left, splits), memoized(right, splits))(node.loc)
          else {
            val right2 = memoized(right, splits)
            
            right2 match {
              case _: Memoize | _: Sort | _: SortBy | _: ReSortBy | _: LoadLocal =>
                Join(op, CrossLeftSort, memoized(left, splits), right2)(node.loc)
              
              case _ =>
                Join(op, CrossLeftSort, memoized(left, splits), Memoize(right2, 100))(node.loc)
            }
          }
        }

        case node @ Join(op, joinSort, left, right) =>
          Join(op, joinSort, memoized(left, splits), memoized(right, splits))(node.loc)
        
        case node @ Filter(ValueSort(id), target, boolean) => {
          val target2 = memoized(target, splits)
          val boolean2 = memoized(boolean, splits)
          
          def resortTarget  = ReSortBy(target2, id)
          def resortBoolean = ReSortBy(boolean2, id)
          
          (target2.sorting, boolean2.sorting) match {
            case (ValueSort(`id`), ValueSort(`id`)) => Filter(ValueSort(id), target2, boolean2)(node.loc) 
            case (ValueSort(`id`), _              ) => Filter(ValueSort(id), target2, resortBoolean)(node.loc)
            case (_,               ValueSort(`id`)) => Filter(ValueSort(id), resortTarget, boolean2)(node.loc)
            case _                                  => Filter(ValueSort(id), resortTarget, resortBoolean)(node.loc)
          }
        }
          
        case node @ Filter(IdentitySort, target, boolean) => {
          val target2 = memoized(target, splits)
          val boolean2 = memoized(boolean, splits)
          
          val (targetIndexes, booleanIndexes) = determineSort(target2, boolean2)
          
          val targetPrefix = targetIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          val booleanPrefix = booleanIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          
          def sortTarget     = Sort(target2, targetIndexes)
          def sortBoolean    = Sort(boolean2, booleanIndexes)

          def sortTargetAux  = Sort(target2, Vector(0 until target2.identities.length: _*))
          def sortBooleanAux = Sort(boolean2, Vector(0 until boolean2.identities.length: _*))
          
          (target2.sorting, targetPrefix, boolean2.sorting, booleanPrefix) match {
            case (IdentitySort, true,  IdentitySort, true ) => Filter(IdentitySort, target2, boolean2)(node.loc)
            case (IdentitySort, true,  IdentitySort, false) => Filter(IdentitySort, target2, sortBoolean)(node.loc)
            case (IdentitySort, false, IdentitySort, true ) => Filter(IdentitySort, sortTarget, boolean2)(node.loc)
            case (IdentitySort, false, IdentitySort, false) => Filter(IdentitySort, sortTarget, sortBoolean)(node.loc)
            
            case (_, _,                IdentitySort, true ) => Filter(IdentitySort, sortTargetAux, boolean2)(node.loc)
            case (_, _,                IdentitySort, false) => Filter(IdentitySort, sortTargetAux, sortBoolean)(node.loc)
                                                                      
            case (IdentitySort, true,  _,            _    ) => Filter(IdentitySort, target2, sortBooleanAux)(node.loc)
            case (IdentitySort, false, _,            _    ) => Filter(IdentitySort, sortTarget, sortBooleanAux)(node.loc)
            
            case _                                          => Filter(IdentitySort, sortTargetAux, sortBooleanAux)(node.loc)
          }
        }

        case node @ Filter(joinSort, target, boolean) =>
          Filter(joinSort, memoized(target, splits), memoized(boolean, splits))(node.loc)
        
        case Sort(parent, _) => memoized(parent, splits)
        
        case SortBy(parent, sortField, valueField, id) =>
          SortBy(memoized(parent, splits), sortField, valueField, id)
        
        case ReSortBy(parent, id) =>
          ReSortBy(memoized(parent, splits), id)
        
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
    val leftPairs = (left2.identities, right2.identities) match {
      case (Identities.Specs(a), Identities.Specs(b)) =>
        a.zipWithIndex filter {
          case (p, i) => b contains p
        }
      case (Identities.Undefined, _) | (_, Identities.Undefined) => Vector.empty
    }

    val rightPairs = (right2.identities, left2.identities) match {
      case (Identities.Specs(a), Identities.Specs(b)) =>
        a.zipWithIndex filter {
          case (p, i) => b contains p
        }
      case (Identities.Undefined, _) | (_, Identities.Undefined) => Vector.empty
    }
    
    val (_, leftIndices) = leftPairs.unzip
    
    val (_, rightIndices) = rightPairs sortWith {
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

    (leftIndices, rightIndices)
  }
}
