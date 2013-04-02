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
package com.precog.accounts

import com.precog.common._
import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.accounts._

import org.specs2.mutable._
import org.specs2.specification.{Fragments, Step}
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Validation, Success, NonEmptyList}
import scalaz.Scalaz._
import scalaz.Validation._

import blueeyes.akka_testing._

import blueeyes.core.data._
import blueeyes.bkka._
import blueeyes.core.service._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import blueeyes.util.Clock

import DefaultBijections._

import org.apache.commons.codec.binary.Base64

import scalaz._
import scalaz.syntax.comonad._

trait TestAccountService extends BlueEyesServiceSpecification with AccountService with AkkaDefaults {

  implicit def executionContext = defaultFutureDispatch
  implicit def M: Monad[Future] with Comonad[Future] = new UnsafeFutureComonad(executionContext, Duration(5, "seconds"))

  val config = """
    security {
      test = true
      mongo {
        mock = true
        servers = [localhost]
        database = test
      }
    }

    accounts {
      rootAccountId = 0000000001
    }
  """

  override val configuration = "services { accounts { v1 { " + config + " } } }"

  val accountManager = new InMemoryAccountManager()(M)
  def AccountManager(config: Configuration) = (accountManager, Stoppable.Noop)
  val apiKeyManager = new InMemoryAPIKeyManager(blueeyes.util.Clock.System)(M)
  def APIKeyFinder(config: Configuration) = new DirectAPIKeyFinder(apiKeyManager)
  def RootKey(config: Configuration) = M.copoint(apiKeyManager.rootAPIKey)

  val clock = Clock.System

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  val rootUser = "root@precog.com"
  val rootPass = "root"

  override def map(fs: => Fragments) = Step {
    accountManager.setAccount("0000000001", rootUser, rootPass, new DateTime, AccountPlan.Root, None)
  } ^ super.map(fs)
}

class AccountServiceSpec extends TestAccountService with Tags {
  def accounts = client.contentType[JValue](application/(MimeTypes.json)).path("/accounts/v1/accounts/")

  def auth(user: String, pass: String): HttpHeader = {
    val raw = (user + ":" + pass).getBytes("utf-8")
    val encoded = Base64.encodeBase64String(raw)
    HttpHeaders.Authorization("Basic " + encoded)
  }

  def listAccounts(request: JValue) =
    accounts.query("", "").post("")(request)

  def createAccount(email: String, password: String) = {
    val request: JValue = JObject(JField("email", JString(email)) :: JField("password", JString(password)) :: Nil)
    accounts.post("")(request)
  }

  def getAccount(accountId: String, user: String, pass: String) =
    accounts.header(auth(user, pass)).get(accountId)

  def getAccountByAPIKey(apiKey: APIKey, user: String, pass: String) =
    accounts.header(auth(user, pass)).query("apiKey", apiKey).get("")

  def deleteAccount(accountId: String, user: String, pass: String) =
    accounts.header(auth(user, pass)).delete(accountId)

  def changePassword(accountId: String, user: String, oldPass: String, newPass: String) = {
    val request: JValue = JObject(JField("password", JString(newPass)) :: Nil)
    accounts.header(auth(user, oldPass)).put(accountId + "/password")(request)
  }

  def addGrantToAccount(accountId: String,request: JValue) =
    accounts.query("accountId",accountId).post(accountId + "/grants/")(request)

  def getAccountPlan(accountId: String, user: String, pass: String) =
    accounts.header(auth(user, pass)).get(accountId + "/plan")

  def putAccountPlan(accountId: String, user: String, pass: String, planType: String) = {
    val request: JValue = JObject(JField("type", JString(planType)) :: Nil)
    accounts.header(auth(user, pass)).put(accountId + "/plan")(request)
  }

  def removeAccountPlan(accountId: String, user: String, pass: String) =
    accounts.header(auth(user, pass)).delete(accountId + "/plan")

  def createAccountAndGetId(email: String, pass: String): Future[String] = {
    createAccount(email, pass) map {
      case HttpResponse(HttpStatus(OK, _), _, Some(jv), _) =>
        val JString(id) = jv \ "accountId"
        id

      case error => 
        sys.error("Invalid response from server when creating account: " + error)
    }
  }

  "accounts service" should {
    "create accounts" in {
      createAccount("test0001@email.com", "12345").copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          jvalue \ "accountId" must beLike { case JString(id) => ok }
      }
    }

    "not create duplicate accounts" in {
      val msgFuture = for {
        HttpResponse(HttpStatus(OK, _), _, Some(jv1), _) <- createAccount("test0002@email.com", "password1")
        HttpResponse(HttpStatus(Conflict, _), _, Some(errorMessage), _) <- createAccount("test0002@email.com", "password2")
      } yield errorMessage

      msgFuture.copoint must_== JString("An account already exists for user test0002@email.com")
    }

    "find own account" in {
      val (user, pass) = ("test0003@email.com", "password")
      (for {
        id <- createAccountAndGetId(user, pass)
        resp <- getAccount(id, user, pass)
      } yield resp).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jv), _) =>
          jv \ "email" must_== JString(user)
      }
    }

    "delete own account" in {
      val (user, pass) = ("test0004@email.com", "password")
      (for {
        id <- createAccountAndGetId(user, pass)
        res0 <- deleteAccount(id, user, pass)
        res1 <- getAccount(id, user, pass)
      } yield ((res0, res1))).copoint must beLike {
        case (HttpResponse(HttpStatus(NoContent, _), _, _, _), HttpResponse(HttpStatus(Unauthorized, _), _, _, _)) =>
          ok
      }
    }

    "change password of account" in {
      val (user, oldPass) = ("test0005@email.com", "password")
      val newPass = "super"
      (for {
        id <- createAccountAndGetId(user, oldPass)
        res0 <- changePassword(id, user, oldPass, newPass)
        res1 <- getAccount(id, user, oldPass)
        res2 <- getAccount(id, user, newPass)
      } yield ((res0, res1, res2))).copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, _, _),
              HttpResponse(HttpStatus(Unauthorized, _), _, _, _),
              HttpResponse(HttpStatus(OK, _), _, _, _)) =>
          ok
      }
    }

    "get account plan type" in {
      val (user, pass) = ("test0006@email.com", "password")
      (for {
        id <- createAccountAndGetId(user, pass)
        res <- getAccountPlan(id, user, pass)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jv), _) =>
          jv \ "type" must_== JString("Free")
      }
    }

    "update account plan type" in {
      val (user, pass) = ("test0007@email.com", "password")
      (for {
        id <- createAccountAndGetId(user, pass)
        res0 <- putAccountPlan(id, user, pass, "Root")
        res1 <- getAccountPlan(id, user, pass)
      } yield ((res0, res1))).copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, _, _),
              HttpResponse(HttpStatus(OK, _), _, Some(jv), _)) =>
          jv \ "type" must_== JString("Root")
      }
    }

    "delete account plan" in {
      val (user, pass) = ("test0008@email.com", "password")
      (for {
        id <- createAccountAndGetId(user, pass)
        _ <- putAccountPlan(id, user, pass, "Root")
        _ <- removeAccountPlan(id, user, pass)
        res <- getAccountPlan(id, user, pass)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jv), _) =>
          jv \ "type" must_== JString("Free")
      }
    }

    "locate account by api key (account api key or subordinates)" in {
      val user = "test0009@email.com"
      val pass = "12345"

      val accountId = createAccountAndGetId(user, pass).copoint

      val JString(apiKey) = getAccount(accountId, user, pass).map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) => jvalue \ "apiKey"
      }.copoint

      val subkey = apiKeyManager.newAPIKey(Some("subkey"), None, apiKey, Set.empty).copoint

      getAccountByAPIKey(subkey.apiKey, rootUser, rootPass).map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) => jvalue \ "accountId"
      }.copoint mustEqual JString(accountId)
    }

    "not find other account" in {
      val (user, pass) = ("test0010@email.com", "password")
      (for {
        id1 <- createAccountAndGetId(user, pass)
        id2 <- createAccountAndGetId("some-other-email@email.com", "password")
        resp <- getAccount(id2, user, pass)
      } yield resp).copoint must beLike {
        case HttpResponse(HttpStatus(Unauthorized, _), _, Some(jv), _) =>
          ok
      }
    }

  }
}
