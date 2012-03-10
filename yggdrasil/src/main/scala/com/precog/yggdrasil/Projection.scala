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

import com.precog.util._
import com.precog.common.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import java.nio.ByteBuffer

import scalaz._
import scalaz.Scalaz._
import scalaz.effect._
import scalaz.iteratee._

trait Sync[A] {
  type X = Throwable
  def read: Option[IO[Validation[X, A]]]
  def sync(a: A): IO[Validation[Throwable,Unit]]
}

trait Projection {
  type X = Throwable

  def descriptor: ProjectionDescriptor

  def chunkSize: Int

  def getAllPairs(expiresAt: Long) : EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO]
  def getAllColumnPairs(columnIndex: Int, expiresAt: Long) : EnumeratorP[X, Vector[(Identities, CValue)], IO]

  def getAllIds(expiresAt: Long) : EnumeratorP[X, Vector[Identities], IO]
  def getAllValues(expiresAt: Long) : EnumeratorP[X, Vector[Seq[CValue]], IO]

  def getColumnValues(path: Path, selector: JPath, expiresAt: Long): EnumeratorP[X, Vector[(Identities, CValue)], IO] = {
    @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes

    val columnIndex = descriptor.columns.indexWhere(col => col.path == path && isEqualOrChild(selector, col.selector))
    getAllColumnPairs(columnIndex, expiresAt)
  }

//  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO]
//
//  /**
//   * Retrieve all IDs for IDs in the given range [start,end]
//   */  
//  def getIdsInRange[X](range : Interval[Identities]) : EnumeratorP[X, Vector[Identities], IO] =
//    getPairsByIdRange(range) map( _.map { case (id, _) => id })
//
//  /**
//   * Retrieve all values for IDs in the given range [start,end]
//   */  
//  def getValuesByIdRange[X](range: Interval[Identities]) : EnumeratorP[X, Vector[Seq[CValue]], IO] =
//    getPairsByIdRange(range) map( _.map { case (_, b) => b })
//
//  def getPairForId[X](id: Identities): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = 
//    getPairsByIdRange(Interval(Some(id), Some(id)))
//
//  def getValueForId[X](id: Identities): EnumeratorP[X, Vector[Seq[CValue]], IO] = 
//    getValuesByIdRange(Interval(Some(id), Some(id)))
}
