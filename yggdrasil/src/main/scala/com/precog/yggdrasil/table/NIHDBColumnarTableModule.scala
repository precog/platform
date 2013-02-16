package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common.security._

import org.joda.time.DateTime

import java.io.File

import scalaz._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import TableModule._

trait NIHDBColumnarTableModule[M[+_], Long] extends BlockStoreColumnarTableModule[M] with RawProjectionModule[M, Long, Slice] {
  def accessControl: AccessControl[M]

  trait NIHDBColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): M[Table] = {
      val constraints = Schema.flatten(tpe).map { case (p, t) => ColumnRef(p, t) }.toSet

      for {
        paths          <- pathsM(table)
        projections    <- paths.map { path =>
          Projection(path) flatMap {
            case Some(proj) =>
              accessControl.hasCapability(apiKey, Set(ReducePermission(path, proj.descriptor.authorities.ownerAccountIds)), Some(new DateTime)) map { canAccess =>
                if (canAccess) Some(proj) else {
                  Projection.close(proj)
                  None
                }
              }
            case None =>
              M.point(None)
          }
        }.sequence map (_.flatten)
      } yield {
        def slices(proj: Projection): StreamT[M, Slice] = {
          StreamT.unfoldM[M, Slice, Option[Long]](None) { key =>
            proj.getBlockAfter(key, constraints).map(_.map { case BlockProjectionData(_, maxKey, slice) => (slice, Some(maxKey)) })
          }
        }

        Table(projections.foldLeft(StreamT.empty[M, Slice]) { (acc, proj) => acc ++ slices(proj) }, ExactSize(projections.map(_.length).sum))
      }
    }
  }
}
