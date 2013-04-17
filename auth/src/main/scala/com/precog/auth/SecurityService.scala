package com.precog.auth

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.services.{CORSHeaderHandler, PathServiceCombinators}

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{DefaultBijections, ByteChunk}
import blueeyes.core.http._
import blueeyes.json._
import blueeyes.health.metrics.eternity
import ByteChunk._
import DefaultBijections._

import org.streum.configrity.Configuration
import scalaz._

trait SecurityService extends BlueEyesServiceBuilder with APIKeyServiceCombinators with PathServiceCombinators {
  case class State(handlers: SecurityServiceHandlers, stoppable: Stoppable)

  val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def APIKeyManager(config: Configuration): (APIKeyManager[Future], Stoppable)
  def clock: blueeyes.util.Clock

  def securityService = service("security", "1.0") {
    requestLogging(timeout) { help("/docs/api") {
      healthMonitor("/health", timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val securityConfig = config.detach("security")
          val (apiKeyManager, stoppable) = APIKeyManager(securityConfig)
          M.point(State(new SecurityServiceHandlers(apiKeyManager, clock), stoppable))
        } ->
        request { case State(handlers, stoppable) =>
          import CORSHeaderHandler.allowOrigin
          import handlers._
          allowOrigin("*", executionContext) {
            jsonp {
              jvalue[ByteChunk] {
                path("/apikeys/'apikey") {
                  get(ReadAPIKeyDetailsHandler) ~
                  delete(DeleteAPIKeyHandler) ~
                  path("/grants/") {
                    get(ReadAPIKeyGrantsHandler) ~
                    post(CreateAPIKeyGrantHandler) ~
                    path("'grantId") {
                      delete(DeleteAPIKeyGrantHandler)
                    }
                  }
                } ~
                path("/grants/'grantId") {
                  get(ReadGrantDetailsHandler) ~
                  path("/children/") {
                    get(ReadGrantChildrenHandler)
                  }
                } ~
                jsonAPIKey(k => handlers.apiKeyManager.findAPIKey(k).map(_.map(_.apiKey))) {
                  path("/apikeys/") {
                    get(ReadAPIKeysHandler) ~
                    post(CreateAPIKeyHandler)
                  } ~
                  path("/grants/") {
                    get(ReadGrantsHandler) ~
                    post(CreateGrantHandler) ~
                    path("'grantId") {
                      delete(DeleteGrantHandler) ~
                      path("/children/") {
                        post(CreateGrantChildHandler)
                      }
                    }
                  } ~
                  dataPath("/permissions/fs") {
                    get(ReadPermissionsHandler)
                  }
                }
              }
            }
          }
        } ->
        stop[State] { case State(_, stoppable) =>
          stoppable
        }
      }
    }}
  }
}
