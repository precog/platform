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
package com.precog.common

import com.precog.util._

import java.nio.ByteBuffer

import blueeyes.json._
import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.http._

import akka.dispatch.{ Future, ExecutionContext }

import scalaz._

trait JValueByteChunkTranscoders {
  private implicit val seqJValueMonoid = new Monoid[Seq[JValue]] {
    def zero = Seq.empty[JValue]
    def append(xs: Seq[JValue], ys: => Seq[JValue]) = xs ++ ys
  }

  implicit def JValueByteChunkTranscoder(implicit M: Monad[Future]) = new AsyncHttpTranscoder[JValue, ByteChunk] {
    def apply(req: HttpRequest[JValue]): HttpRequest[ByteChunk] =
      req.copy(content = req.content.map { (j: JValue) =>
        Left(j.renderCompact.getBytes("UTF-8"))
      })

    def unapply(fres: Future[HttpResponse[ByteChunk]]): Future[HttpResponse[JValue]] = {
      fres.flatMap { res =>
        res.content match {
          case Some(bc) =>
            val fv: Future[Validation[Seq[Throwable], JValue]] =
              JsonUtil.parseSingleFromByteChunk(bc)
            fv.map(v => res.copy(content = v.toOption))
          case None =>
            M.point(res.copy(content = None))
        }
      }
    }
  }
}

object JValueByteChunkTranscoders extends JValueByteChunkTranscoders


