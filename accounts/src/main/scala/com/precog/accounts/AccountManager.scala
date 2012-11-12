package com.precog
package accounts

import com.precog.common.Path
import com.precog.common.security._

import org.joda.time.DateTime
import org.bson.types.ObjectId

trait AccountManager[M[+_]] {
  def newAccountId: M[AccountID]
  
  def newTempPassword(): String = new ObjectId().toString

  def updateAccount(account: Account): M[Boolean]
  def updateAccountPassword(account: Account, newPassword: String): M[Boolean]
 
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan)(f: (AccountID, Path) => M[APIKey]): M[Account]

  def listAccountIds(apiKey: APIKey) : M[Set[Account]]
  
  def findAccountById(accountId: AccountID): M[Option[Account]]
  def findAccountByEmail(email: String) : M[Option[Account]]
  def authAccount(email: String, password: String) : M[Option[Account]]
  
  def deleteAccount(accountId: AccountID): M[Option[Account]]

  def close(): M[Unit]
} 
