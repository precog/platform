package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common._
import com.precog.common.security._
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.yggdrasil.vfs._

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Future, Promise}
import akka.pattern.AskSupport
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import java.io.File

import scalaz._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import TableModule._

trait VFSColumnarTableModule extends BlockStoreColumnarTableModule[Future] with SecureVFSModule[Future, Slice] with AskSupport with Logging {
  def vfs: SecureVFS

  trait VFSColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): EitherT[Future, ResourceError, Table] = {
      logger.debug("Starting load from " + table.toJson)
      for {
        paths <- EitherT.right(pathsM(table))
        projections <- paths.toList.traverse[({ type l[a] = EitherT[Future, ResourceError, a] })#l, ProjectionLike[Future, Slice]] { path =>
          logger.debug("  Loading path: " + path)
          vfs.readProjection(apiKey, path, Version.Current, AccessMode.Read)
        }
      } yield {
        val length = projections.map(_.length).sum
        logger.debug("Loading from projections: " + projections)
        val stream = projections.foldLeft(StreamT.empty[Future, Slice]) { (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints = proj.structure.map { struct => 
            Some(Schema.flatten(tpe, struct.toList)) 
          }

          acc ++ StreamT.wrapEffect(constraints map { c => proj.getBlockStream(c) })
        }

        Table(stream, ExactSize(length))
      } 
    }
  }
}
