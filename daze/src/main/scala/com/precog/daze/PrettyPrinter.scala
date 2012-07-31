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
          wrap(depth, "UnfixedSolution(line, "+id+",\n", prettyPrintAux(target, bindings, depth+1), ")")
  
        case Extra(target) =>
          wrap(depth, "Target(line,\n", prettyPrintAux(target, bindings, depth+1), ")")
      }) +
      (if(suffixNL) "\n" else "")
    }
    
    def prettyPrintProvenance(provenance: Provenance) = provenance match {
      case StaticProvenance(path) => "StaticProvenance("+prettyString(path)+")"
      case other => other.toString
    }

    def prettyPrintAux(graph: DepGraph, bindings: Map[DepGraph, String], depth: Int, prefixIndent: Boolean = true, suffixNL: Boolean = true, resolveBinding: Boolean = true): String = {
      (if(prefixIndent) mkIndent(depth) else "") +
      (if (resolveBinding && bindings.contains(graph))
        bindings(graph)
      else {
        graph match {
          case Root(_, instr) => "Root(line, "+(instr match {
            case PushString(str) => "PushString("+prettyString(str)+")"
            case PushNum(numStr) => "PushNum("+prettyString(numStr)+")"
            case other => other
          })+")"

          case New(_, parent) => wrap(depth, "New(line,\n", prettyPrintAux(parent, bindings, depth+1), ")")
  
          case LoadLocal(_, parent, _) => wrap(depth, "LoadLocal(line,\n", prettyPrintAux(parent, bindings, depth+1), ")")
  
          case Operate(_, op, parent) => wrap(depth, "Operate(line, "+op+",\n", prettyPrintAux(parent, bindings, depth+1), ")")
  
          case Reduce(_, red, parent) => wrap(depth, "Reduce(line, "+red+",\n", prettyPrintAux(parent, bindings, depth+1), ")")
          
          case Morph1(_, m, parent) => wrap(depth, "Morph1(line, "+m+",\n", prettyPrintAux(parent, bindings, depth+1), ")")
  
          case Morph2(_, m, left, right) =>
            wrap(depth, "Morph2(line, "+m+",\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")")
              
          case Join(_, op, joinSort, left, right) =>
            wrap(depth, "Join(line, "+op+", "+joinSort+",\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")")
  
          case IUI(_, union, left, right) =>
            wrap(depth, "IUI(line, "+union+",\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")")
              
          case Diff(_, left, right) =>
            wrap(depth, "Diff(line,\n",
              prettyPrintAux(left, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(right, bindings, depth+1), ")")
  
          case Filter(_, filterSort, target, boolean) =>
            wrap(depth, "Filter(line, "+filterSort+",\n",
              prettyPrintAux(target, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(boolean, bindings, depth+1), ")")

          case Sort(parent, indices) =>
            wrap(depth, "Sort(\n",
              prettyPrintAux(parent, bindings, depth+1, suffixNL = false)+",\n"+mkIndent(depth+1)+indices+"\n", ")")
  
          case SortBy(parent, sortField, valueField, id) =>
            wrap(depth, "SortBy(\n",
              prettyPrintAux(parent, bindings, depth+1, suffixNL = false)+",\n"+
                mkIndent(depth+1)+prettyString(sortField)+", "+prettyString(valueField)+", "+id+"\n", ")")
  
          case Memoize(parent, priority) =>
            wrap(depth, "Memoize(\n",
              prettyPrintAux(parent, bindings, depth+1, suffixNL = false)+",\n"+mkIndent(depth+1)+priority+"\n", ")")
  
          case Distinct(_, parent) => wrap(depth, "Distinct(line,\n", prettyPrintAux(parent, bindings, depth+1), ")")
          
          case Split(_, spec, child) =>
            wrap(depth, "Split(line,\n", prettyPrintSpec(spec, bindings, depth+1, suffixNL = false)+",\n"+prettyPrintAux(child, bindings, depth+1), ")")
            
          case sp @ SplitParam(_, id) => "SplitParam(line, "+id+")("+bindings(sp.parent)+")" 
  
          case sp @ SplitGroup(_, id, provenance) => "SplitParam(line, "+id+", "+provenance.map(prettyPrintProvenance)+"("+bindings(sp.parent)+")" 
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
    
          case New(_, parent) => collectBindings(parent, counts0)
  
          case LoadLocal(_, parent, _) => collectBindings(parent, counts0)
  
          case Operate(_, _, parent) => collectBindings(parent, counts0)
  
          case Reduce(_, _, parent) => collectBindings(parent, counts0)
          
          case Morph1(_, _, parent) => collectBindings(parent, counts0)
  
          case Morph2(_, _, left, right) => collectBindings(right, collectBindings(left, counts0))
              
          case Join(_, _, _, left, right) => collectBindings(right, collectBindings(left, counts0))
  
          case IUI(_, _, left, right) => collectBindings(right, collectBindings(left, counts0))
              
          case Diff(_, left, right) => collectBindings(right, collectBindings(left, counts0))
  
          case Filter(_, _, target, boolean) => collectBindings(boolean, collectBindings(target, counts0))
          
          case Sort(parent, _) => collectBindings(parent, counts0)
  
          case SortBy(parent, _, _, _) => collectBindings(parent, counts0)
              
          case Memoize(parent, _) => collectBindings(parent, counts0)
  
          case Distinct(_, parent) => collectBindings(parent, counts0)
          
          case Split(_, spec, child) => collectBindings(child, collectSpecBindings(spec, counts0))
  
          case sp @ SplitParam(_, _) => incrementValue(sp.parent, counts0)

          case sg @ SplitGroup(_, _, _) => incrementValue(sg.parent, counts0)
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