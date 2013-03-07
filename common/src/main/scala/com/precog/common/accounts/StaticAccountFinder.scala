package com.precog.common
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

import com.precog.common.security._

class StaticAccountFinder[M[+_]: Monad](accountId: AccountId) extends AccountFinder[M] with Logging {
  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(accountId))

  def findAccountByAPIKey(apiKey: APIKey) = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId) = None.point[M]
}
