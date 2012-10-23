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
package com.precog.ingest
package service

import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.data._

import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import scalaz._

case class DecompressService[B](delegate: HttpService[ByteChunk, B])(implicit ctx: ExecutionContext)
extends DelegatingService[ByteChunk, B, ByteChunk, B] with Logging {
  import HttpStatusCodes._
  import HttpHeaders._
  import Encodings.{ identity => _, _ }
  import Validation.{ failure, success }

  def service = { (request: HttpRequest[ByteChunk]) =>
    val decompress: Validation[NotServed, ByteChunk => ByteChunk] =
      request.headers.header[`Content-Encoding`] match {
        case Some(contentEncoding) =>
          contentEncoding.encodings match {
            case Seq(`gzip`) =>
              success(GunzipByteChunk(_))
            case Seq(`deflate`) =>
              success(InflateByteChunk(_))
            case Seq(Encodings.`identity`) =>
              success(identity)
            case _ =>
              failure(DispatchError(BadRequest, "Cannot handle encoding: " + contentEncoding))
          }
        case None =>
          success(identity)
      }

    decompress flatMap { decompress =>
      delegate.service(request.copy(content = request.content map (decompress)))
    }
  }

  val metadata = None
}

trait DecompressCombinators extends blueeyes.bkka.AkkaDefaults {
  def decompress[A](service: HttpService[ByteChunk, A]) = new DecompressService(service)
}

