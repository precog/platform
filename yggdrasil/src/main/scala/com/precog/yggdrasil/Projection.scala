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
import com.precog.analytics.Path

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
  def read: Option[IO[Validation[String, A]]]
  def sync(a: A): IO[Validation[Throwable,Unit]]
}

trait Projection {
  def getAllPairs[X] : EnumeratorP[X, (Identities, Seq[CValue]), IO]
  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, (Identities, Seq[CValue]), IO]
  def getPairForId[X](id: Identities): EnumeratorP[X, (Identities, Seq[CValue]), IO]
  def getColumnValues[X](path: Path, selector: JPath): EnumeratorP[X, (Identities, CValue), IO]

  def getAllIds[X] : EnumeratorP[X, Identities, IO] 
  def getIdsInRange[X](range : Interval[Identities]) : EnumeratorP[X, Identities, IO] 

  def getAllValues[X] : EnumeratorP[X, Seq[CValue], IO] 
  def getValuesByIdRange[X](range: Interval[Identities]) : EnumeratorP[X, Seq[CValue], IO]
  def getValueForId[X](id: Identities): EnumeratorP[X, Seq[CValue], IO] 
}
