package com.precog.yggdrasil
package metadata

import com.precog.common._
import com.precog.common.security._
import com.precog.yggdrasil.vfs._

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

case class PathStructure(types: Map[CType, Long], children: Set[CPath])

object PathStructure {
  val Empty = PathStructure(Map.empty, Set.empty)
}
