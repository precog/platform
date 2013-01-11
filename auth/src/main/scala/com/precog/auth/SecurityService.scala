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
          M.point((new SecurityServiceHandlers(apiKeyManager), stoppable))
        } ->
        request { case (handlers, stoppable) =>
          import handlers._
          jsonp[ByteChunk] {
            transcode {
              apiKey(state.apiKeyManagement.apiKeyManager) {
                path("/apikeys/") {
                  get(ReadAPIKeysHandler) ~
                  post(CreateAPIKeyHandler) ~
                  path("'apikey") {
                    get(ReadAPIKeyDetailsHandler) ~
                    delete(DeleteAPIKeyHandler) ~
                    path("/grants/") {
                      get(ReadAPIKeyGrantsHandler) ~
                      post(CreateAPIKeyGrantHandler) ~
                      path("'grantId") {
                        delete(DeleteAPIKeyGrantHandler)
                      }
                    } ~
                  }
                } ~
                path("/grants/") {
                  get(ReadGrantsHandler) ~
                  post(CreateGrantHandler) ~
                  path("'grantId") {
                    get(ReadGrantDetailsHandler) ~
                    delete(DeleteGrantHandler) ~
                    path("/children/") {
                      get(ReadGrantChildrenHandler) ~
                      post(CreateGrantChildHandler)
                    }
                  }
                }
              }
            }
          }
        } ->
        stop { case (_, stoppable) =>
          stoppable
        }
      }
    }
  }
}
