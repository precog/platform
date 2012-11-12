package com.precog
package accounts

import com.precog.common.Path
import com.precog.common.security._

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb

import org.joda.time.DateTime
import org.streum.configrity.Configuration

import scalaz._

case class AccountManagerClientSettings(
  protocol: String = "http",
  host: String = "localhost",
  port: Int = 80,
  path: String = "/accounts/v1/")

object AccountManagerClientSettings {
  val defaults = AccountManagerClientSettings()
}

trait AccountManagerClientComponent {
  implicit def asyncContext: ExecutionContext
  implicit val M: Monad[Future]

  def accountManagerFactory(config: Configuration): AccountManager[Future] = {
    val protocol = config[String]("security.service.protocol", "http")
    val host = config[String]("security.service.host", "localhost")
    val port = config[Int]("security.service.port", 80)
    val path = config[String]("security.service.path", "/accounts/v1/")
    
    val settings = AccountManagerClientSettings(protocol, host, port, path)
    new AccountManagerClient(settings)
  }
}

class AccountManagerClient(settings: AccountManagerClientSettings) extends AccountManager[Future] {
  import settings._
  
  def newAccountId: Future[AccountID] = sys.error("TODO")
  
  def updateAccount(account: Account): Future[Boolean] = sys.error("TODO")
  def updateAccountPassword(account: Account, newPassword: String): Future[Boolean] = sys.error("TODO")
 
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan)(f: (AccountID, Path) => Future[APIKey]): Future[Account] = sys.error("TODO")

  def listAccountIds(apiKey: APIKey) : Future[Set[Account]] = sys.error("TODO")
  
  def findAccountById(accountId: AccountID): Future[Option[Account]] = sys.error("TODO")
  def findAccountByEmail(email: String) : Future[Option[Account]] = sys.error("TODO")
  def authAccount(email: String, password: String) : Future[Option[Account]] = sys.error("TODO")
  
  def deleteAccount(accountId: AccountID): Future[Option[Account]] = sys.error("TODO")

  def close(): Future[Unit] = sys.error("TODO")
  
  def withClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
}
