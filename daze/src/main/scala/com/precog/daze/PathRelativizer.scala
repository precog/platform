package com.precog
package daze

import scala.collection.mutable
import com.precog.common.Path

trait PathRelativizer[M[+_]] extends DAG with StringLib[M] {
  import dag._
  import instructions._

  def makePathRelative(graph: DepGraph, prefix: Path): DepGraph = {
    graph.mapDown { recurse => {
      case dag.LoadLocal(loc, parent, jtpe) => {
        dag.LoadLocal(loc, 
          dag.Join(loc, BuiltInFunction2Op(concat), CrossRightSort, 
            Root(loc, PushString(prefix.toString())), 
            recurse(parent)), 
          jtpe)
      }
    }}
  }
}
