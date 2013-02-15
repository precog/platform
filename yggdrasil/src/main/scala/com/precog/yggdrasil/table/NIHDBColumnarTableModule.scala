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

import TableModule._

trait NIHDBColumnarTableModule[M[+_], Long] extends BlockStoreColumnarTableModule[M] with ProjectionModule[M, Key, Slice] with StorageMetadataSource[M] {
  def accessControl: Accesscontrol[M]

  def load(table: Table, apiKey: APIKey, tpe: JType): M[Table] = {
    import loadMergeEngine._

    val constraints = Schema.flatten(tpe)

    for {
      paths          <- pathsM(table)
      projections    <- paths.map { path =>
        for {
          proj <- Projection(path)
          canAccess <- accessControl.hasCapability(apiKey, Set(ReducePermission(path, proj.ownerAccountIds)), some(new DateTime))
        } yield {
          if (canAccess) Some(proj) else {
            close(proj)
            None
          }
        }
      }.sequence map (_.flatten)
    } yield {
      def slices(proj: Projection): StreamT[M, Slice] = {
        StreamT.unfoldM[M, Slice, Option[Long]](None) { key =>
          proj.getBlockAfter(key, constraints).map(_.map { case BlockProjectionData(_, maxKey, slice) => (slice, Some(maxKey)) })
        }
      }

      Table(projections.foldLeft(StreamT.empty[M, Slice]) { (acc, proj) => acc ++ slices(proj) })
    }
  }


}
