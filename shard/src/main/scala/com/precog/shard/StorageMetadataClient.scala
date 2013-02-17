package com.precog.shard

import com.precog.common._
import com.precog.common.json._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.muspelheim._

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.Validation._
import scalaz.std.anyVal._
import scalaz.std.iterable._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._

class StorageMetadataClient[M[+_]: Monad](metadata: StorageMetadataSource[M]) extends MetadataClient[M] {
  def browse(userUID: String, path: Path): M[Validation[String, JArray]] = {
    metadata.userMetadataView(userUID).findChildren(path) map {
      case paths => success(JArray(paths.map( p => JString(p.toString)).toSeq: _*))
    }
  }

  def structure(userUID: String, path: Path, property: CPath): M[Validation[String, JObject]] = {
    sys.error("FIXME for NIHDB")
//    val futRoot = metadata.userMetadataView(userUID).findPathMetadata(path, property)
//
//    def transform(children: Set[PathMetadata]): JObject = {
//      val (childNames, types) = children.foldLeft((Set.empty[String], Map.empty[String, Long])) {
//        case ((cs, ts), PathIndex(i, _)) =>
//          val path = "[%d]".format(i)
//          (cs + path, ts)
//
//        case ((cs, ts), PathField(f, _)) =>
//          val path = "." + f
//          (cs + path, ts)
//      
//        case ((cs, ts), PathValue(t, _, descriptors)) => 
//          val tname = CType.nameOf(t)
//          val counts = for {
//            colMetadata <- descriptors.values
//            (colDesc, metadata) <- colMetadata if colDesc.valueType == t
//            stats <- metadata.values collect { case s: MetadataStats => s }
//          } yield stats.count
//
//          (cs, ts + (tname -> (ts.getOrElse(tname, 0L) + counts.suml)))
//      }
//
//      JObject("children" -> childNames.serialize, "types" -> types.serialize)
//    }
//
//    futRoot.map { pr => Success(transform(pr.children)) }
  }
}

// vim: set ts=4 sw=4 et:
