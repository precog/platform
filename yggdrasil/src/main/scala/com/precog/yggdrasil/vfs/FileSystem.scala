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
import com.precog.common.security._
import com.precog.util.PrecogUnit
import com.precog.util.cache.Cache
import com.precog.yggdrasil.nihdb._

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
case class NIHDBData(data: Seq[(Long, Seq[JValue])]) extends PathData(PathData.NIHDB)

object NIHDBData {
  val Empty = NIHDBData(Seq.empty)
}

sealed trait PathOp {
  def path: Path
}

sealed trait PathUpdateOp extends PathOp

/**
  * Appends data to a resource. If the streamId is non-empty, this
  * Append is part of an atomic version update sequence (see
  * [[com.precog.yggdrasil.vfs.Create]] for details on the
  * semantics). If the streamId is empty, then this Append is applied
  * to the current HEAD version. If there is no current version
  * available, a new version will be created as long as the apiKey has
  * create permissions for the path.
  */
case class Append(path: Path, data: PathData, streamId: APIKey \/ UUID, jobId: Option[JobId], authorities: Authorities') extends PathUpdateOp

/**
  * Creates a new version of the given resource based on the
  * streamId. This message represents an atomic version update
  * sequence. In an atomic version update sequence, the Create will
  * generate a new version, but that version is not automatically
  * promoted to HEAD. The Create must be followed by zero or more
  * Append messages with matching streamIds, and then a final Replace
  * message with matching streamId to indicate promotion to HEAD.
  */
case class CreateNewVersion(path: Path, apiKey: APIKey, data: PathData, streamId: UUID, authorities: Authorities, canOverwrite: Boolean) extends PathUpdateOp

/**
  * Replace the current HEAD with the version specified by the streamId.
  */
case class SetCurrentVersion(path: Path, streamId: UUID, jobId: JobId) extends PathUpdateOp

case class Read(path: Path, streamId: Option[UUID], auth: Option[APIKey]) extends PathOp
case class ReadProjection(path: Path, streamId: Option[UUID], auth: Option[APIKey]) extends PathOp
case class Execute(path: Path, auth: Option[APIKey]) extends PathOp
case class Stat(path: Path, auth: Option[APIKey]) extends PathOp

case class FindChildren(path: Path, auth: APIKey) extends PathOp



sealed trait PathActionResponse

case class UpdateSuccess(path: Path) extends PathActionResponse
case class UpdateFailure(path: Path, error: ResourceError) extends PathActionResponse

sealed trait ReadResult extends PathActionResponse {
  def resources: List[Resource]
}

case class ReadSuccess(path: Path, resources: List[Resource]) extends ReadResult
case class ReadFailure(path: Path, errors: NEL[ResourceError]) extends ReadResult {
  val resources = Nil
}

sealed trait ReadProjectionResult extends PathActionResponse {
  def projection: Option[NIHDBProjection]
}

case class ReadProjectionSuccess(path: Path, projection: Option[NIHDBProjection]) extends ReadProjectionResult
case class ReadProjectionFailure(path: Path, messages: NEL[ResourceError]) extends ReadProjectionResult {
  val projection = None
}

/* class FileSystem */
