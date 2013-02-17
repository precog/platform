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
import com.precog.yggdrasil.nihdb._

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Future, Promise}
import akka.pattern.AskSupport
import akka.util.Timeout

import org.joda.time.DateTime

import java.io.File

import scalaz._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import TableModule._

trait NIHDBColumnarTableModule extends BlockStoreColumnarTableModule[Future] with AskSupport { 
  def accessControl: AccessControl[Future]
  def actorSystem: ActorSystem
  def projectionsActor: ActorRef
  def storageTimeout: Timeout

  trait NIHDBColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
      val constraints = Some(Schema.flatten(tpe).map { case (p, t) => ColumnRef(p, t) }.toSet)

      for {
        paths          <- pathsM(table)
        projections    <- paths.map { path =>
          implicit val timeout = storageTimeout
          (projectionsActor ? AccessProjection(path, apiKey)).mapTo[Option[NIHDBProjection]]
        }.sequence map (_.flatten)
        totalLength    <- projections.map(_.length).sequence.map(_.sum)
      } yield {
        def slices(proj: NIHDBProjection): StreamT[Future, Slice] = {
          StreamT.unfoldM[Future, Slice, Option[Long]](None) { key =>
            proj.getBlockAfter(key, constraints).map(_.map { case BlockProjectionData(_, maxKey, slice) => (slice, Some(maxKey)) })
          }
        }

        Table(projections.foldLeft(StreamT.empty[Future, Slice]) { (acc, proj) => acc ++ slices(proj) }, ExactSize(totalLength))
      }
    }
  }
}
