package com.precog.common
package accounts

import com.precog.common.security._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.monad._

trait AccountFinder[M[+_]] extends Logging { self =>
  def findAccountByAPIKey(apiKey: APIKey) : M[Option[AccountId]]

  def findAccountById(accountId: AccountId): M[Option[Account]]

  def resolveForWrite(accountId: Option[AccountId], apiKey: APIKey)(implicit M0: Monad[M]): M[Option[AccountId]] = {
    accountId map { accountId0 =>
      // this is just a sanity check to ensure that the specified 
      // account id actually exists.
      logger.debug("Using provided ownerAccountId: " + accountId0)
      findAccountById(accountId0) map { _ map { _.accountId } }    
    } getOrElse {
      logger.debug("Looking up accounts based on apiKey " + apiKey)
      findAccountByAPIKey(apiKey) 
    }
  }
  
  def mapAccountIds(apiKeys: Set[APIKey])(implicit M0: Monad[M]) : M[Map[APIKey, AccountId]] = {
    apiKeys.foldLeft(Map.empty[APIKey, AccountId].point[M]) {
      case (macc, key) => 
        for {
          m <- macc
          ids <- findAccountByAPIKey(key)
        } yield {
          m ++ ids.map(key -> _)
        }
    }
  }

  def withM[N[+_]](implicit t: M ~> N) = new AccountFinder[N] {
    def findAccountByAPIKey(apiKey: APIKey) = t(self.findAccountByAPIKey(apiKey))

    def findAccountById(accountId: AccountId) = t(self.findAccountById(accountId))
  }
}

object AccountFinder {
  def Empty[M[+_]: Monad] = new AccountFinder[M] {
    def findAccountByAPIKey(apiKey: APIKey) = None.point[M]
    def findAccountById(accountId: AccountId) = None.point[M]
  }
}


// vim: set ts=4 sw=4 et:
