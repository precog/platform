package com.precog
package auth

import common.security._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{DefaultBijections, ByteChunk}
import blueeyes.health.metrics.eternity
import ByteChunk._

import org.streum.configrity.Configuration
import scalaz._

case class SecurityServiceState(apiKeyManagement: APIKeyManagement, stoppable: Stoppable)

trait SecurityService extends BlueEyesServiceBuilder with APIKeyServiceCombinators {
  import DefaultBijections._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit val M: Monad[Future]

  def APIKeyManager(config: Configuration): (APIKeyManager[Future], Stoppable)

  val securityService = service("security", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val securityConfig = config.detach("security")
          val (apiKeyManager, stoppable) = APIKeyManager(securityConfig)
          M.point(SecurityServiceState(new APIKeyManagement(apiKeyManager), stoppable))
        } ->
        request { (state: SecurityServiceState) =>
          jsonp[ByteChunk] {
            transcode {
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
          }
        } ->
        stop { state => 
          state.stoppable
        }
      }
    }
  }
}
