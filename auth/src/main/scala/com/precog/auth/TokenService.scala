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
          val tokenManager = tokenManagerFactory(config.detach("security"))

          val accessControl = new TokenManagerAccessControl(tokenManager)

          Future(TokenServiceState(tokenManager, accessControl))
        } ->
        request { (state: TokenServiceState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              path("/token") {
                get(new GetTokenHandler(state.tokenManager)) ~
                post(new AddTokenHandler(state.tokenManager)) ~
                path("/grants") {
                  get(new GetGrantsHandler(state.tokenManager)) ~
                  post(new AddGrantHandler(state.tokenManager)) ~
                  path("/'grantId") {
                    delete(new RemoveGrantHandler(state.tokenManager)) ~
                    path("/children") {
                      get(new GetGrantChildrenHandler(state.tokenManager)) ~
                      post(new AddGrantChildrenHandler(state.tokenManager)) ~
                      path("/'childGrantId") {
                        get(new GetGrantChildHandler(state.tokenManager)) ~
                        delete(new RemoveGrantChildHandler(state.tokenManager))
                      }
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
