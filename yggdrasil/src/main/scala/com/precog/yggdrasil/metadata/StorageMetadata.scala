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

trait StorageMetadata[M[+_]] { self =>

  /**
   * Returns the direct children of path.
   *
   * The results are the basenames of the children. So for example, if
   * we have /foo/bar/qux and /foo/baz/duh, and path=/foo, we will
   * return (bar, baz).
   */
  def findDirectChildren(path: Path): M[Set[Path]]

  def findSelectors(path: Path): M[Set[CPath]]
  def findSize(path: Path): M[Long]
  def findStructure(path: Path, selector: CPath): M[PathStructure]

  def currentAuthorities(path: Path): M[Option[Authorities]]
  def currentVersion(path: Path): M[Option[VersionEntry]]

  def liftM[T[_[+_], +_]](implicit T: Hoist[T], M0: Monad[M]) = new StorageMetadata[({ type λ[+α] = T[M, α] })#λ] {
    def findDirectChildren(path: Path) = self.findDirectChildren(path).liftM[T]
    def findSelectors(path: Path) = self.findSelectors(path).liftM[T]
    def findSize(path: Path) = self.findSize(path).liftM[T]
    def currentAuthorities(path: Path)  = self.currentAuthorities(path).liftM[T]
    def currentVersion(path: Path) = self.currentVersion(path).liftM[T]
    def findStructure(path: Path, selector: CPath) = self.findStructure(path, selector).liftM[T]
  }
}
