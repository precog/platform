package com.precog.common
package security

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.bkka._

import org.streum.configrity.Configuration

trait WebAPIKeyFinderComponent {
  def APIKeyFinder(config: Configuration): APIKeyFinder[Future] = {
    sys.error("todo")
  }
}

class WebAPIKeyFinder()(implicit executor: ExecutionContext) extends APIKeyFinder[Future] {
  val M = new FutureMonad(executor)
  def findAPIKey(apiKey: APIKey): Future[Option[APIKeyRecord]] = sys.error("todo")
  def findGrant(gid: GrantId): Future[Option[Grant]] = sys.error("todo")
}



// vim: set ts=4 sw=4 et:
