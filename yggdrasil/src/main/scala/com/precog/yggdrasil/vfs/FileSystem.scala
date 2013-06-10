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
import DefaultSerialization._

import com.google.common.cache.RemovalCause

import com.precog.common.Path
import com.precog.common.accounts.AccountId
import com.precog.common.ingest.FileContent
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.niflheim.NIHDB
import com.precog.yggdrasil.nihdb._
import com.precog.util.PrecogUnit
import com.precog.util.cache.Cache

import java.util.UUID

import org.joda.time.DateTime

import scalaz.{NonEmptyList => NEL, _}
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._

sealed class PathData(val typeName: PathData.DataType)
object PathData {
  sealed abstract class DataType(name: String)
  object DataType {
    implicit val decomposer: Decomposer[DataType] with Extractor[DataType] = new Decomposer[DataType] with Extractor[DataType] {
      def decompose(t: DataType) = t match {
        case BLOB(contentType) => JObject("type" -> JString("blob"), "mimeType" -> JString(contentType.value))
        case NIHDB => JObject("type" -> JString("nihdb"), "mimeType" -> JString(FileContent.XQuirrelData.value))
      }

      def validated(v: JValue) = {
        val mimeTypeV = v.validated[String]("mimeType").flatMap { mimeString =>
          MimeTypes.parseMimeTypes(mimeString).headOption.toSuccess(Extractor.Error.invalid("No recognized mimeType values foundin %s".format(v.renderCompact)))
        }

        (v.validated[String]("type") tuple mimeTypeV) flatMap {
          case ("blob", mimeType) => success(BLOB(mimeType))
          case ("nihdb", FileContent.XQuirrelData) => success(NIHDB)
          case (unknownType, mimeType) => failure(Extractor.Error.invalid("Data type %s (mimetype %s) is not a recognized PathData datatype".format(unknownType, mimeType.toString)))
        }
      }
    }
  }
  case class BLOB(contentType: MimeType) extends DataType("blob")
  case object NIHDB extends DataType("nihdb")
}

case class BlobData(data: Array[Byte], mimeType: MimeType) extends PathData(PathData.BLOB(mimeType))
case class NIHDBData(data: Seq[NIHDB.Batch]) extends PathData(PathData.NIHDB)

object NIHDBData {
  val Empty = NIHDBData(Seq.empty)
}

sealed trait PathOp {
  def path: Path
}

case class Read(path: Path, version: Version) extends PathOp 
case class FindChildren(path: Path) extends PathOp //FIXME: remove auth
case class CurrentVersion(path: Path) extends PathOp 


/* class FileSystem */
