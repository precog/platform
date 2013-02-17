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
