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

trait NIHDBColumnarTableModule extends BlockStoreColumnarTableModule[Future] with AskSupport with Logging {
  def secureVFS: SecureVFS[Future]
  def storageTimeout: Timeout

  trait NIHDBColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      logger.debug("Starting load from " + table.toJson)
      val tableV = for {
        paths          <- EitherT.right(pathsM(table))
        projections    <- paths.toList.traverse { path =>
                            logger.debug("  Loading path: " + path)
                            secureVFS.readProjection(apiKey, path, Version.Current)
                          } 
        length = projections.map(_.length).sum
      } yield {
        logger.debug("Loading from projections: " + projections)
        Table(projections.foldLeft(StreamT.empty[Future, Slice]) { (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints = proj.structure.map { struct => Some(Schema.flatten(tpe, struct.toList).map { case (p, t) => ColumnRef(p, t) }.toSet) }
          acc ++ StreamT.wrapEffect(constraints map { c => proj.getBlockStream(c) })
        }, ExactSize(length))
      }

      tableV.getOrElse(Table.empty)


    }
  }
}
