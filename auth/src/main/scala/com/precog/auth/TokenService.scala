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

case class TokenServiceState(tokenManager: TokenManager)

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
          Future(TokenServiceState(tokenManagerFactory(config.detach("tokenManager"))))
        } ->
        request { (state: TokenServiceState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              path("tokens") {
                get(new GetTokenHandler(state.tokenManager)) ~
                post(new CreateTokenHandler(state.tokenManager)) ~
                delete(new DeleteTokenHandler(state.tokenManager)) ~
                post(new UpdateTokenHandler(state.tokenManager))
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}

// Token Services
//
//
// Get token details
//
// GET tokens?tokenId={token}
//
//
// New Token 
//
// POST tokens?tokenId={parent token} 
//
//
// Delete token
//
// DELETE tokens?tokenId={authority}&delete={token-to-delete}
//
//
// Update token {protected fields}
//
// POST tokens?tokenId={authority}&update{token-to-update}
//
