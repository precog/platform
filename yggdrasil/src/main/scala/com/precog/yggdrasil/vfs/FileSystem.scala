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
package com.precog
package yggdrasil
package vfs

import akka.actor.Actor
import akka.dispatch.{Await, ExecutionContext, Future, Promise}
import akka.pattern.pipe
import akka.util.Duration

import blueeyes.bkka.FutureMonad
import blueeyes.core.http._
import blueeyes.json._
import blueeyes.json.serialization._

import com.google.common.cache.RemovalCause

import com.precog.common.Path
import com.precog.common.accounts.AccountId
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.niflheim.NIHDB
import com.precog.yggdrasil.nihdb._
import com.precog.util.PrecogUnit
import com.precog.util.cache.Cache
import Resource._

import java.util.UUID

import org.joda.time.DateTime

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._

sealed class PathData(val typeName: String)
object PathData {
  final val BLOB = "blob"
  final val NIHDB = "nihdb"
}

case class BlobData(data: Array[Byte], mimeType: MimeType) extends PathData(PathData.BLOB)
case class NIHDBData(data: Seq[NIHDB.Batch]) extends PathData(PathData.NIHDB)

object NIHDBData {
  val Empty = NIHDBData(Seq.empty)
}

sealed trait PathOp {
  def path: Path
}

case class Read(path: Path, version: Version) extends PathOp 
case class FindChildren(path: Path) extends PathOp
case class CurrentVersion(path: Path) extends PathOp 


sealed trait PathActionResponse

case class PathFailure(path: Path, error: ResourceError) extends PathActionResponse
case class UpdateSuccess(path: Path) extends PathActionResponse
case class UpdateFailure(path: Path, error: ResourceError) extends PathActionResponse

sealed trait ReadResult extends PathActionResponse

case class ReadSuccess(path: Path, resource: Resource) extends ReadResult
case class ReadFailure(path: Path, error: ResourceError) extends ReadResult

/* class FileSystem */
