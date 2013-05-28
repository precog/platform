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


