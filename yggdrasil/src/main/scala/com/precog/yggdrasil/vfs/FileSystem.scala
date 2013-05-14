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

case class Read(path: Path, version: Version, auth: Option[APIKey]) extends PathOp 
case class ReadProjection(path: Path, version: Version, auth: Option[APIKey]) extends PathOp 
case class Execute(path: Path, auth: Option[APIKey]) extends PathOp 
case class Stat(path: Path, auth: Option[APIKey]) extends PathOp 
case class FindChildren(path: Path, auth: APIKey) extends PathOp
case class CurrentVersion(path: Path, auth: APIKey) extends PathOp 

sealed trait PathActionResponse

case class PathFailure(path: Path, errors: NEL[ResourceError]) extends PathActionResponse
case class UpdateSuccess(path: Path) extends PathActionResponse
case class UpdateFailure(path: Path, errors: NEL[ResourceError]) extends PathActionResponse

sealed trait ReadResult extends PathActionResponse {
  def resource: Option[Resource]
}

case class ReadSuccess(path: Path, resource: Option[Resource]) extends ReadResult
case class ReadFailure(path: Path, errors: NEL[ResourceError]) extends ReadResult {
  val resource = None
}

sealed trait ReadProjectionResult extends PathActionResponse {
  def projection: Option[NIHDBProjection]
}

case class ReadProjectionSuccess(path: Path, projection: Option[NIHDBProjection]) extends ReadProjectionResult
case class ReadProjectionFailure(path: Path, messages: NEL[ResourceError]) extends ReadProjectionResult {
  val projection = None
}

/* class FileSystem */
