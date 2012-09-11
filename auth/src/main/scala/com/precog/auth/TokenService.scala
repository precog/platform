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

case class TokenServiceState(tokenManagement: TokenManagement)

trait TokenService extends BlueEyesServiceBuilder with AkkaDefaults with TokenServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  def tokenManagerFactory(config: Configuration): TokenManager[Future]

  val tokenService = service("auth", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val securityConfig = config.detach("security")
          val tokenManager = tokenManagerFactory(securityConfig)

          Future(TokenServiceState(new TokenManagement(tokenManager)))
        } ->
        request { (state: TokenServiceState) =>
          jsonp[ByteChunk] {
            token(state.tokenManagement.tokenManager) {
              path("/auth") {
                path("/apikeys/") {
                  post(new CreateTokenHandler(state.tokenManagement)) ~
                  path("'apikey") {
                    get(new GetTokenDetailsHandler(state.tokenManagement)) ~
                    delete(new DeleteTokenHandler(state.tokenManagement)) ~
                    path("/grants/") {
                      get(new GetTokenGrantsHandler(state.tokenManagement)) ~
                      post(new AddTokenGrantHandler(state.tokenManagement)) ~
                      path("'grantId") {
                        delete(new RemoveTokenGrantHandler(state.tokenManagement))
                      }
                    }
                  }
                } ~
                path("/grants/") {
                  post(new CreateGrantHandler(state.tokenManagement)) ~
                  path("'grantId") {
                    get(new GetGrantDetailsHandler(state.tokenManagement)) ~
                    delete(new DeleteGrantHandler(state.tokenManagement)) ~
                    path("/children/") {
                      get(new GetGrantChildrenHandler(state.tokenManagement)) ~
                      post(new AddGrantChildHandler(state.tokenManagement))
                    }
                  }
                }
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
