package com.precog.yggdrasil
package metadata

import com.precog.common.Path
import com.precog.common.json.CPath

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

trait StorageMetadata[M[+_]] { self =>
  implicit def M: Monad[M]

  def findChildren(path: Path): M[Set[Path]]
  def findSelectors(path: Path): M[Set[CPath]]
//  def findProjections(path: Path, selector: CPath): M[Map[ProjectionDescriptor, ColumnMetadata]]
//  def findPathMetadata(path: Path, selector: CPath): M[PathRoot]
//
//  def findProjections(path: Path): M[Map[ProjectionDescriptor, ColumnMetadata]] = {
//    findSelectors(path) flatMap { selectors => 
//      selectors.traverse(findProjections(path, _)) map { proj =>
//        if(proj.size == 0) {
//          Map.empty[ProjectionDescriptor, ColumnMetadata]
//        } else {
//          proj.reduce(_ ++ _) 
//        }
//      }
//    }
//  }
//
//  def findProjections(path: Path, selector: CPath, valueType: CType): M[Map[ProjectionDescriptor, ColumnMetadata]] = 
//    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }
//
//  def typeFilter(path: Path, selector: CPath, valueType: CType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
//    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType == valueType )
//  }


  def liftM[T[_[+_], +_]](implicit T: Hoist[T]) = new StorageMetadata[({ type λ[+α] = T[M, α] })#λ] {
    private implicit val M0: Monad[M] = self.M
    val M: Monad[({ type λ[+α] = T[M, α] })#λ] = T(M0)

    def findChildren(path: Path) = self.findChildren(path).liftM[T]
    def findSelectors(path: Path) = self.findSelectors(path).liftM[T]
//    def findProjections(path: Path, selector: CPath) = self.findProjections(path, selector).liftM[T]
//    def findPathMetadata(path: Path, selector: CPath) = self.findPathMetadata(path, selector).liftM
//
//    override def findProjections(path: Path) = self.findProjections(path).liftM
//
//    override def findProjections(path: Path, selector: CPath, valueType: CType) = self.findProjections(path, selector, valueType).liftM
//
//    override def typeFilter(path: Path, selector: CPath, valueType: CType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean =
//      self.typeFilter(path, selector, valueType)(t)
  }
}
