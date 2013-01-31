package com.precog.heimdall

import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.client._
import com.precog.common.JValueByteChunkTranscoders._
import BaseClient.Response

import blueeyes._
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes.{ Response => _, _ }
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import scalaz._

import WebJobManager._

trait AuthService[M[+_]] { self =>
  def isValid(apiKey: APIKey): M[Boolean]

  def withM[N[+_]](implicit t: M ~> N) = new AuthService[N] {
    def isValid(apiKey: APIKey): N[Boolean] = t(self.isValid(apiKey))
  }
}

case class WebAuthService(protocol: String, host: String, port: Int, path: String)(implicit executor: ExecutionContext)
    extends WebClient(protocol, host, port, path) with AuthService[Response] {
  import scalaz.syntax.monad._
  import scalaz.EitherT.eitherT
  implicit val M: Monad[Future] = new FutureMonad(executor)

  final def isValid(apiKey: APIKey): Response[Boolean] = withJsonClient { client =>
    eitherT(client.query("apiKey", apiKey).get[JValue]("apikeys/" + apiKey) map {
      case HttpResponse(HttpStatus(OK, _), _, _, _) => \/.right(true)
      case HttpResponse(HttpStatus(NotFound, _), _, _, _) => \/.right(false)
      case res => \/.left("Unexpected response from auth service:\n" + res)
    })
  }
}

case class TestAuthService[M[+_]](validAPIKeys: Set[APIKey])(implicit M: Pointed[M]) extends AuthService[M] {
  final def isValid(apiKey: APIKey): M[Boolean] = M.point(validAPIKeys contains apiKey)
}

