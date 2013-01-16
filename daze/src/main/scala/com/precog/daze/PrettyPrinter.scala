/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.daze

import org.apache.commons.lang.StringEscapeUtils.escapeJava
import com.precog.yggdrasil._

trait PrettyPrinter extends DAG {
  import dag._
  import instructions.{ PushNum, PushString }
  
  def prettyPrint(graph: DepGraph): String = {
    
    def mkIndent(depth: Int): String = "  "*depth
    def wrap(depth: Int, prefix: String, body: String, suffix: String) = prefix+body+mkIndent(depth)+suffix
    def prettyString(s : String) = "\""+escapeJava(s)+"\""
    
    def prettyPrintBinding(graph: DepGraph, bindings: Map[DepGraph, String]): String = {
      "lazy val "+bindings(graph)+" =\n"+prettyPrintAux(graph, bindings, 1, resolveBinding = false)
    }
    
    def prettyPrintSpec(spec: BucketSpec, bindings: Map[DepGraph, String], depth: Int, prefixIndent: Boolean = true, suffixNL: Boolean = true): String = {
      (if(prefixIndent) mkIndent(depth) else "") +
      (spec match {
        case UnionBucketSpec(left, right) =>
          wrap(depth, "UnionBucketSpec(\n",
            prettyPrintSpec(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintSpec(right, bindings, depth+1), ")")
        
        case IntersectBucketSpec(left, right) =>
          wrap(depth, "IntersectBucketSpec(\n",
            prettyPrintSpec(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintSpec(right, bindings, depth+1), ")")
        
        case Group(id, target, child) =>
          wrap(depth, "Group("+id+", \n",
            prettyPrintAux(target, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintSpec(child, bindings, depth+1), ")")
        
        case UnfixedSolution(id, target) =>
          wrap(depth, "UnfixedSolution("+id+",\n", prettyPrintAux(target, bindings, depth+1), ")")
  
        case Extra(target) =>
          wrap(depth, "Target(line,\n", prettyPrintAux(target, bindings, depth+1), ")")
      }) +
      (if(suffixNL) "\n" else "")
    }
    
    def prettyPrintIdentitySpec(identity: IdentitySpec) = identity match {
      case LoadIds(path) => "LoadIds("+prettyString(path)+")"
      case other => other.toString
    }

    def prettyPrintAux(graph: DepGraph, bindings: Map[DepGraph, String], depth: Int, prefixIndent: Boolean = true, suffixNL: Boolean = true, resolveBinding: Boolean = true): String = {
      (if(prefixIndent) mkIndent(depth) else "") +
      (if (resolveBinding && bindings.contains(graph))
        bindings(graph)
      else {
        graph match {
          case Const(instr) => "Const("+(instr match {
            case CString(str) => "CString("+prettyString(str)+")"
            case other => other
          })+")(line)"

          case Undefined() => "Undefined(line)"

          case New(parent) => wrap(depth, "New(", prettyPrintAux(parent, bindings, depth+1), ")(line)")
  
          case LoadLocal(parent, _) => wrap(depth, "LoadLocal(\n", prettyPrintAux(parent, bindings, depth+1), ")(line)")
  
          case Operate(op, parent) => wrap(depth, "Operate("+op+",\n", prettyPrintAux(parent, bindings, depth+1), ")(line)")
  
          case Reduce(red, parent) => wrap(depth, "Reduce("+red+",\n", prettyPrintAux(parent, bindings, depth+1), ")(line)")

          case MegaReduce(reds, parent) => wrap(depth, "MegaReduce("+reds+",\n", prettyPrintAux(parent, bindings, depth+1), ")(line)")
          
          case Morph1(m, parent) => wrap(depth, "Morph1("+m+",\n", prettyPrintAux(parent, bindings, depth+1), ")(line)")
  
          case Morph2(m, left, right) =>
            wrap(depth, "Morph2("+m+",\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")(line)")
              
          case Join(op, joinSort, left, right) =>
            wrap(depth, "Join("+op+", "+joinSort+",\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")(line)")
  
          case IUI(union, left, right) =>
            wrap(depth, "IUI("+union+",\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")(line)")
              
          case Diff(left, right) =>
            wrap(depth, "Diff(",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")(line)")
  
          case Filter(filterSort, target, boolean) =>
            wrap(depth, "Filter("+filterSort+",\n",
              prettyPrintAux(target, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(boolean, bindings, depth+1), ")(line)")

          case Sort(parent, indices) =>
            wrap(depth, "Sort(\n",
              prettyPrintAux(parent, bindings, depth+1, suffixNL = false)+",\n"+mkIndent(depth+1)+indices+"\n", ")")
  
          case SortBy(parent, sortField, valueField, id) =>
            wrap(depth, "SortBy(\n",
              prettyPrintAux(parent, bindings, depth+1, suffixNL = false)+",\n"+
                mkIndent(depth+1)+prettyString(sortField)+", "+prettyString(valueField)+", "+id+"\n", ")")
                
          case ReSortBy(parent, id) =>
            wrap(depth, "ReSortBy(\n",
              prettyPrintAux(parent, bindings, depth + 1, suffixNL = false) + ",\n" +
                mkIndent(depth + 1) + id, ")")
  
          case Memoize(parent, priority) =>
            wrap(depth, "Memoize(\n",
              prettyPrintAux(parent, bindings, depth+1, suffixNL = false)+",\n"+mkIndent(depth+1)+priority+"\n", ")")
  
          case Distinct(parent) => wrap(depth, "Distinct(", prettyPrintAux(parent, bindings, depth+1), ")(line)")
          
          case Split(spec, child) =>
            wrap(depth, "Split(\n", prettyPrintSpec(spec, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(child, bindings, depth+1), ")(line)")
            
          // not using extractors due to bug
          case sp: SplitParam => "SplitParam("+sp.id+")("+bindings(sp.parent)+")(line)" 
  
          // not using extractors due to bug
          case sp: SplitGroup => "SplitParam("+sp.id+", "+sp.identities.fold(_.map(prettyPrintIdentitySpec), "UndefinedIdentity")+")("+bindings(sp.parent)+")(line)" 
        }
      }) +
      (if(suffixNL) "\n" else "")
    }

    def collectSpecBindings(spec: BucketSpec, counts: Map[DepGraph, Int]): Map[DepGraph, Int] = {
      spec match {
        case UnionBucketSpec(left, right) =>
          collectSpecBindings(right, collectSpecBindings(left, counts)) 
        
        case IntersectBucketSpec(left, right) =>
          collectSpecBindings(right, collectSpecBindings(left, counts)) 
        
        case Group(id, target, child) =>
          collectSpecBindings(child, collectBindings(target, counts)) 
        
        case UnfixedSolution(id, target) =>
          collectBindings(target, counts)
        
        case Extra(target) =>
          collectBindings(target, counts)
      }
    }
    
    def collectBindings(graph: DepGraph, counts: Map[DepGraph, Int]): Map[DepGraph, Int] = {
      
      def incrementValue[K](key: K, map: Map[K, Int]) : Map[K, Int] =
        map.updated(key, map.getOrElse(key, 0)+1)
      
      if (counts.contains(graph)) incrementValue(graph, counts)
      else {
        val counts0 = counts+(graph -> 1)
        graph match {
          case _: Root => counts0
    
          case New(parent) => collectBindings(parent, counts0)
  
          case LoadLocal(parent, _) => collectBindings(parent, counts0)
  
          case Operate(_, parent) => collectBindings(parent, counts0)
  
          case Reduce(_, parent) => collectBindings(parent, counts0)
          
          case MegaReduce(_, parent) => collectBindings(parent, counts0)
          
          case Morph1(_, parent) => collectBindings(parent, counts0)
  
          case Morph2(_, left, right) => collectBindings(right, collectBindings(left, counts0))
              
          case Join(_, _, left, right) => collectBindings(right, collectBindings(left, counts0))
  
          case IUI(_, left, right) => collectBindings(right, collectBindings(left, counts0))
              
          case Diff(left, right) => collectBindings(right, collectBindings(left, counts0))
  
          case Filter(_, target, boolean) => collectBindings(boolean, collectBindings(target, counts0))
          
          case Sort(parent, _) => collectBindings(parent, counts0)
  
          case SortBy(parent, _, _, _) => collectBindings(parent, counts0)
          
          case ReSortBy(parent, _) => collectBindings(parent, counts0)
              
          case Memoize(parent, _) => collectBindings(parent, counts0)
  
          case Distinct(parent) => collectBindings(parent, counts0)
          
          case Split(spec, child) => collectBindings(child, collectSpecBindings(spec, counts0))
  
          // not using extractors due to bug
          case sp: SplitParam => incrementValue(sp.parent, counts0)

          // not using extractors due to bug
          case sg: SplitGroup => incrementValue(sg.parent, counts0)
        }
      }
    }
    
    val rawBindings = collectBindings(graph, Map()).flatMap(x => if(x._2 > 1 && x._1 != graph) Some(x._1) else None).toList
    val (splits, shared) = rawBindings.partition { case _ : Split => true ; case _ => false }
    
    val splitBindings = splits match {
      case Nil => Map.empty[DepGraph, String]
      case s :: Nil => Map(s -> "split")
      case ss => ss.zipWithIndex.toMap.mapValues("split"+_)
    }

    val sharedBindings = shared match {
      case Nil => Map.empty[DepGraph, String]
      case s :: Nil => Map(s -> "node")
      case ss => ss.zipWithIndex.toMap.mapValues("node"+_)
    }
    
    val bindings = (splitBindings ++ sharedBindings) + (graph -> "input")

    """val line = Line(0, "")"""+"\n\n"+
    (splitBindings.toSeq.sortBy(_._2).map(_._1).map(prettyPrintBinding(_, bindings)) ++
     sharedBindings.toSeq.sortBy(_._2).map(_._1).map(prettyPrintBinding(_, bindings)) ++
     List(prettyPrintBinding(graph, bindings))).reduce(_+"\n"+_)
  }
}
