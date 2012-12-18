package com.precog.common
package accounts

import com.precog.common.security._

import scalaz._
import scalaz.syntax.monad._

trait AccountFinder[M[+_]] {
  implicit val M: Monad[M]

  def findAccountByAPIKey(apiKey: APIKey) : M[Option[AccountId]]

  def findAccountById(accountId: AccountId): M[Option[Account]]

  def mapAccountIds(apiKeys: Set[APIKey]) : M[Map[APIKey, AccountId]] = {
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
}


// vim: set ts=4 sw=4 et:
