package com.precog.common
package security

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.bkka._

import org.streum.configrity.Configuration
import scalaz._

object WebAPIKeyFinder {
  def apply(config: Configuration)(implicit M: Monad[Future]): APIKeyFinder[Future] = {
    sys.error("todo")
  }
}

class WebAPIKeyFinder()(implicit val M: Monad[Future]) extends APIKeyFinder[Future] {
  def findAPIKey(apiKey: APIKey): Future[Option[APIKeyRecord]] = sys.error("todo")
  def findGrant(gid: GrantId): Future[Option[Grant]] = sys.error("todo")
}



// vim: set ts=4 sw=4 et:
