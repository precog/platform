package com.precog.ingest
package service

import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.data._

import akka.dispatch.ExecutionContext

import scalaz._

case class DecompressService[B](delegate: HttpService[ByteChunk, B])(implicit ctx: ExecutionContext)
extends DelegatingService[ByteChunk, B, ByteChunk, B] {
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

