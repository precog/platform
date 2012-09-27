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

import com.precog.auth._
import com.precog.common._
import com.precog.common.Path
import com.precog.common.security._

import org.specs2.mutable._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Validation, Success, NonEmptyList}
import scalaz.Scalaz._
import scalaz.Validation._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.AkkaDefaults
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import blueeyes.json.JsonAST._

import blueeyes.util.Clock

/*
//we need to create a security server as well as the accounts server, because the accounts server relies on the security server

trait TestAPIKeyService extends BlueEyesServiceSpecification with APIKeyService with AkkaDefaults with MongoAPIKeyManagerComponent {

  val asyncContext = defaultFutureDispatch

  import BijectionsChunkJson._

  val config = """ 
    security {
      test = true
      mongo {
        mock = true
        servers = [localhost]
        database = test
      }
    }   
  """

  override val configuration = "services { auth { v1 { " + config + " } } }"

  override def apiKeyManagerFactory(config: Configuration) = TestAPIKeyManager.testAPIKeyManager[Future]

  lazy val authService = service.contentType[JValue](application/(MimeTypes.json)).path("/auth")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}


//object TestAPIKeyServer extends TestAPIKeyService

trait TestAccountService extends BlueEyesServiceSpecification with AccountService  with AkkaDefaults with ZKMongoAccountManagerComponent {

  val asyncContext = defaultFutureDispatch

  import BijectionsChunkJson._

  val config = """ 
    security {
      test = true
      mongo {
        mock = true
        servers = [localhost]
        database = test
      }
    }   
  """

  override val configuration = "services { auth { v1 { " + config + " } } }"

  override def AccountManagerFactory(config: Configuration) = TestAccountManager.testAccountManager[Future]

  lazy val accountService = service.contentType[JValue](application/(MimeTypes.json)).path("/accounts")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}



//not complete

class TServiceSpec extends TestAPIKeyService with  FutureMatchers with Tags {
import TestAPIKeyManager._
 
}
object TServiceSpec

class AccountServiceSpec extends TestAccountService with  FutureMatchers with Tags {
  import TestAccountManager._
 
  
  def listAccounts(request: JValue) = 
    accountService.query("", "").post("")(request)

  def createAccount(email: String, request: JValue) = 
    accountService.query("email", email).post("")(request)


  
  
  def getAccountDetails(accountId: String, request: JValue) = 
    accountService.query("accountId",accountId).post(accountId)(request)
  
  //def updateAccount(account: Account, queryKey: String) = 
 //   accountService.query("account", account.serialize).put(accountId + "/apikeys/")

  
  def deleteAccount(accountId: String) = 
    accountService.query("accountId",accountId).delete(accountId)

  
  
  
  
  def addGrantToAccount(accountId: String,request: JValue) = 
    accountService.query("accountId",accountId).post(accountId + "/grants/")(request)

  
  
  def getAccountPlan(accountId: String) = 
    accountService.query("accountId",accountId).get(accountId + "/plan/")
  
  def putAccountPlan(accountId: String, planType: String, request: JValue) = 
    accountService.query("type", planType).put(accountId + "/plan/")(request)
  
  def removeAccountPlan(accountId: String) = 
    accountService.query("accountId",accountId).delete(accountId + "/plan/")
  
  
  
  "Account service" should {
    
    "Create an Account" in {
      createAccount("email",JObject(List(JField("","")))) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(accountID), _) => ok
      }}
    }

    
    "Delete an Account" in {
      
      //create account
      //delete account
      //find account
//      deleteAccount("todo create one first") must whenDelivered { beLike {
  //      case HttpResponse(HttpStatus(OK, _), _, _, _) => ok
    //  }}
    }
    
    
     "List Accounts" in {
       //create list of accounts
       
       //list accounts, check accountIds
      listAccounts(JObject(List(JField("","")))) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, _, _) => ok
      }}
    }
    
    //TODO : more to add here once APIKey Server can be started
    
  }
  
}
*/
