package com.precog.yggdrasil
package util

import com.precog.common.json._
import blueeyes.json._

object CPathUtils {
  def cPathToJPaths(cpath: CPath, value: CValue): List[(JPath, CValue)] = (cpath.nodes, value) match {
    case (CPathField(name) :: tail, _) => addComponent(JPathField(name), cPathToJPaths(CPath(tail), value))
    case (CPathIndex(i) :: tail, _) => addComponent(JPathIndex(i), cPathToJPaths(CPath(tail), value))
    case (CPathArray :: tail, CArray(elems, CArrayType(elemType))) =>
      elems.toList.zipWithIndex flatMap { case (e, i) => addComponent(JPathIndex(i), cPathToJPaths(CPath(tail), elemType(e))) }
    // case (CPathMeta(_) :: _, _) => Nil
    case (Nil, _) => List((JPath.Identity, value))
    case (path, _) => sys.error("Bad news, bob! " + path)
  }

  private def addComponent(c: JPathNode, xs: List[(JPath, CValue)]): List[(JPath, CValue)] = xs map {
    case (path, value) => (JPath(c :: path.nodes), value)
  }
}
