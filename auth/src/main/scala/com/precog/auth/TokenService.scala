package com.precog
package auth

import common.security._

import blueeyes.BlueEyesServer

import akka.dispatch.Future

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.health.metrics.{eternity}

import org.streum.configrity.Configuration

case class TokenServiceState(tokenManager: TokenManager, accessControl: AccessControl)

trait TokenService extends BlueEyesServiceBuilder with AkkaDefaults with TokenServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  def tokenManagerFactory(config: Configuration): TokenManager

  val tokenService = service("auth", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val theTokenManager = tokenManagerFactory(config.detach("tokenManager"))

          val accessControl = new TokenBasedAccessControl {
            val executionContext = defaultFutureDispatch
            val tokenManager = theTokenManager
          }

          Future(TokenServiceState(theTokenManager, accessControl))
        } ->
        request { (state: TokenServiceState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              path("/tokens") {
                get(new GetTokenHandler(state.tokenManager)) ~
                // Note the update handler needs to be before the create
                // handler as it's arguements are a super set of the create
                // call thus requires first refusal
                post(new UpdateTokenHandler(state.tokenManager)) ~
                post(new CreateTokenHandler(state.tokenManager, state.accessControl)) ~
                delete(new DeleteTokenHandler(state.tokenManager)) 
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
