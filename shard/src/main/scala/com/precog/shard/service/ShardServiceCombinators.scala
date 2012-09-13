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
package com.precog.shard
package service

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.JsonAST.JValue
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.precog.common.Path
import com.precog.common.security._
import com.precog.ingest.service._

import scalaz.Validation
import scalaz.Validation._

trait ShardServiceCombinators extends IngestServiceCombinators {

  type Query = String

  import BijectionsByteArray._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  // def query[A, B](next: HttpService[A, (Token, Path, Query) => Validation[NotServed, Future[B]]]) = {
  //   new DelegatingService[A, (Token, Path) => Future[B], A, (Token, Path, Query) => Future[B]] {
  //     val delegate = next
  //     val metadata = None
  //     val service = (request: HttpRequest[A]) => {
  //       val query: Option[String] = request.parameters.get('q).filter(_ != null)
  //       // val limit: Option[String] = request.parameters.get('limit).filter(_ != null)
  //       // val offset: Option[String] = request.parameters.get('offset).filter(_ != null)

  //       query map { q =>
  //         next.service(request) flatMap { f =>
  //           (token: Token, path: Path) => f(token, path, q)
  //         }
  //       } getOrElse {
  //         failure(inapplicable)
  //       }
  //     }
  //   }
  // }

  def query[A, B](next: HttpService[A, (Token, Path, Query) => Future[B]]) = {
    new DelegatingService[A, (Token, Path) => Future[B], A, (Token, Path, Query) => Future[B]] {
      val delegate = next
      val metadata = None
      val service = (request: HttpRequest[A]) => {
        val query: Option[String] = request.parameters.get('q).filter(_ != null)

        query map { q =>
          next.service(request) map { f => (token: Token, path: Path) => f(token, path, q) }
        } getOrElse {
          failure(inapplicable)
        }
      }
    }
  }

  def jsonpcb[A](delegate: HttpService[Future[JValue], Future[HttpResponse[A]]])(implicit bi: Bijection[A, ByteChunk]) =
    jsonpc[Array[Byte], Array[Byte]](delegate map (_ map { response =>
      response.copy(content = response.content map (bi(_)))
    }))
}
