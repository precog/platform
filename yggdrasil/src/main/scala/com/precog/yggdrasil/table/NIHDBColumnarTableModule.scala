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
