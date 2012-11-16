/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

class AccountManagerClient(settings: AccountManagerClientSettings) extends AccountManager[Future] with AkkaDefaults {
  import settings._

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
  
  def updateAccount(account: Account): Future[Boolean] = sys.error("TODO 1")
 
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountID])(f: (AccountID, Path) => Future[APIKey]): Future[Account] = sys.error("TODO 3")

  def listAccountIds(apiKey: APIKey) : Future[Set[Account]] = sys.error("TODO 4")
  
  def findAccountById(accountId: AccountID): Future[Option[Account]] = sys.error("TODO 5")

  def findAccountByEmail(email: String) : Future[Option[Account]] = sys.error("TODO 6")
  
  def deleteAccount(accountId: AccountID): Future[Option[Account]] = sys.error("TODO 8")

  def close(): Future[Unit] = sys.error("TODO 9")
  
  def withClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
}
