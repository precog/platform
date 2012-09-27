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

case class SecurityServiceState(apiKeyManagement: APIKeyManagement)

trait SecurityService extends BlueEyesServiceBuilder with AkkaDefaults with APIKeyServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]

  val securityService = service("security", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val securityConfig = config.detach("security")
          val apiKeyManager = apiKeyManagerFactory(securityConfig)
          Future(SecurityServiceState(new APIKeyManagement(apiKeyManager)))
        } ->
        request { (state: SecurityServiceState) =>
          jsonp[ByteChunk] {
            apiKey(state.apiKeyManagement.apiKeyManager) {
              path("/apikeys/") {
                get(new GetAPIKeysHandler(state.apiKeyManagement)) ~
                post(new CreateAPIKeyHandler(state.apiKeyManagement)) ~
                path("'apikey") {
                  get(new GetAPIKeyDetailsHandler(state.apiKeyManagement)) ~
                  delete(new DeleteAPIKeyHandler(state.apiKeyManagement)) ~
                  path("/grants/") {
                    get(new GetAPIKeyGrantsHandler(state.apiKeyManagement)) ~
                    post(new AddAPIKeyGrantHandler(state.apiKeyManagement)) ~
                    path("'grantId") {
                      delete(new RemoveAPIKeyGrantHandler(state.apiKeyManagement))
                    }
                  }
                }
              } ~
              path("/grants/") {
                post(new CreateGrantHandler(state.apiKeyManagement)) ~
                path("'grantId") {
                  get(new GetGrantDetailsHandler(state.apiKeyManagement)) ~
                  delete(new DeleteGrantHandler(state.apiKeyManagement)) ~
                  path("/children/") {
                    get(new GetGrantChildrenHandler(state.apiKeyManagement)) ~
                    post(new AddGrantChildHandler(state.apiKeyManagement))
                  }
                }
              }
            }
          }
        } ->
        shutdown { state => 
          for {
            _ <- state.apiKeyManagement.close()
          } yield Option.empty[Stoppable]
        }
      }
    }
  }
}
