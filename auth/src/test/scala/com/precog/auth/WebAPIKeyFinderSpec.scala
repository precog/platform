package com.precog.auth

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.client._

import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.bkka._

import akka.util.Duration
import akka.dispatch._

import org.streum.configrity.Configuration

import scalaz._
import scalaz.syntax.copointed._

class WebAPIKeyFinderSpec extends APIKeyFinderSpec[Future] with AkkaDefaults { self =>
  implicit lazy val executionContext = defaultFutureDispatch
  implicit lazy val M: Monad[Future] with Copointed[Future] = new FutureMonad(executionContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, Duration(5, "seconds"))
  }

  def withAPIKeyFinder[A](mgr: APIKeyManager[Future])(f: APIKeyFinder[Future] => A): A = {
    val testService = new TestAPIKeyService {
      val apiKeyManager = mgr
    }
    val (service, stoppable) = testService.server(Configuration.parse(testService.config), executionContext).start.get.copoint
    val client = new HttpClient[ByteChunk] {
      def isDefinedAt(req: HttpRequest[ByteChunk]) = true
      def apply(req: HttpRequest[ByteChunk]) =
        service.service(req) getOrElse Future(HttpResponse(HttpStatus(NotFound)))
    }

    val apiKeyFinder = new WebAPIKeyFinder {
      val M = self.M
      val rootAPIKey = self.M.copoint(mgr.rootAPIKey)
      val rootGrantId = self.M.copoint(mgr.rootGrantId)
      val executor = self.executionContext
      protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = f(client.path("/"))
    }

    val result = f(apiKeyFinder.withM[Future])

    stoppable foreach { stop =>
      Stoppable.stop(stop, Duration(1, "minutes")).copoint
    }
    result
  }
}
