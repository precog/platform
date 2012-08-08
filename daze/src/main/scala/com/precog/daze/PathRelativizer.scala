package com.precog
package daze


import scala.collection.mutable
import com.precog.common.Path


trait PathRelativizer[M[+_]] extends DAG with StringLib[M] with CrossOrdering{
  import dag._
  import instructions.Map2CrossRight
  import instructions.Add
  import instructions.PushString
  import instructions.BuiltInFunction2Op
  

  def makePathRelative(graph: DepGraph, prefix: Path) : DepGraph = {
    val memotable = mutable.Map[DepGraph, DepGraph]()

    def copySpec(splits: => Map[Split, Split], spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(copySpec(splits, left), copySpec(splits, right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(copySpec(splits, left), copySpec(splits, right))
      
      case Group(id, target, child) =>
        Group(id, copyAux(splits, target), copySpec(splits, child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, copyAux(splits, target))
      
      case Extra(target) =>
        Extra(copyAux(splits, target))
    }

    def copyAux(splits0: => Map[Split, Split], graph: DepGraph) : DepGraph = {
      

      //lazy val splits, why not use splits0.. because we don't want it to bind until it is run?
      lazy val splits = splits0

      def inner(graph: DepGraph): DepGraph = graph match {
        case r : Root => r
  
        case New(loc, parent) => New(loc, copyAux(splits, parent))

                case LoadLocal(loc, parent, jtpe) =>
               LoadLocal(loc, Join(loc, BuiltInFunction2Op(concat), CrossRightSort, Root(loc, PushString(prefix.toString())), 
               copyAux(splits, parent)), jtpe)

//        case LoadLocal(loc, parent, jtpe) =>
  //             LoadLocal(loc, Join(loc, BuiltInFunction2Op(concat), CrossRightSort, Root(loc, PushString(prefix)), 
    //           copyAux(splits, parent)), jtpe)
               
        case Operate(loc, op, parent) => Operate(loc, op, copyAux(splits, parent))
  
        case Reduce(loc, red, parent) => Reduce(loc, red, copyAux(splits, parent))
  
        case Morph1(loc, m, parent) => Morph1(loc, m, copyAux(splits, parent))
  
        case Morph2(loc, m, left, right) => Morph2(loc, m, copyAux(splits, left), copyAux(splits, right))
  
        //tom added join to this as extra parameter?
        case Join(loc, instr, join, left, right) => Join(loc, instr, join, copyAux(splits, left), copyAux(splits, right))
  
        case Filter(loc, cross, target, boolean) =>
          Filter(loc, cross, copyAux(splits, target), copyAux(splits, boolean))
  
        case Sort(parent, indices) => Sort(copyAux(splits, parent), indices)
  
        case Memoize(parent, priority) => Memoize(copyAux(splits, parent), priority)
  
        case Distinct(loc, parent) => Distinct(loc, copyAux(splits, parent))
  
        case s @ Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> s2)
          lazy val spec2 = copySpec(splits2, spec)
          lazy val child2 = copyAux(splits2, child)
          lazy val s2: Split = Split(loc, spec2, child2)
          s2
        }
  
        case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(splits(s.parent))
  
        case s @ SplitParam(loc, id) => SplitParam(loc, id)(splits(s.parent))
      }

      memotable.get(graph) getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    copyAux(Map(), graph)
  }
}
