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

import org.specs2.execute.Result
import org.specs2.mutable.{After, Specification}
import org.specs2.specification._

import akka.actor.ActorSystem
import akka.util.Duration
import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.ExecutionContext

import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo._

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import org.streum.configrity._

import scalaz._

/*
object MongoAccountManagerSpec extends Specification {
 

  val timeout = Duration(30, "seconds")

  "mongo Account manager" should {
    "find Account using accountId " in new AccountManager { 

      lazy val result = Await.result(accountManager.findAccount(account.accountId), timeout)

      result must beLike {
        case Some(Account(accountId,_,_,_,_,_,_)) => accountId must_== account.accountId
      }
    }
    
     "find Account id using email " in new AccountManager { 

      lazy val result = Await.result(accountManager.findAccountId(account.email), timeout)

      result must beLike {
        case Some(Account(accountId,_,_,_,_,_,_)) => accountId must_== account.accountId
      }
    }
    
    "not find missing Account" in new AccountManager { 

      val result = Await.result(accountManager.findAccount(notFoundAccountID), timeout)

      result must beLike {
        case None => ok 
      }
    }
    
    
    "update Account using accountId " in new AccountManager { 

      val updatedAccount = new Account(account.accountId,account.email, account.password, account.accountCreationDate,"new API key", account.rootPath, account.plan)
      var result = Await.result(accountManager.updateAccount(updatedAccount), timeout)

      val result2 = Await.result(accountManager.findAccount(account.accountId), timeout)

      
      result2 must beLike {
        case Some(Account(accountId,_,_,_,apiKey,_,_)) => apiKey must_== updatedAccount.apiKey
      }
    }
    
    "move Account to deleted pool on deletion" in new AccountManager { 

      type Results = (Option[Account], Option[Account], Option[Account])//, Option[Account])

      val fut: Future[Results] = for { 
        before <- accountManager.findAccount(account.accountId)
        deleted <- accountManager.deleteAccount(before.get.accountId)
        after <- accountManager.findAccount(account.accountId)
       // deleteCol <- accountManager.findDeletedAccount(account.accountId)
      } yield {
        (before, deleted, after)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case (Some(t1), Some(t2), None) => 
          t1 must_== t2
          //t1 must_== t3
      }
    }
    
    "no failure on deleting Account that is already deleted" in new AccountManager { 
      type Results = (Option[Account], Option[Account], Option[Account], Option[Account])//, Option[Account])

      val fut: Future[Results] = for { 
        before <- accountManager.findAccount(account.accountId)
        deleted1 <- accountManager.deleteAccount(before.get.accountId)
        deleted2 <- accountManager.deleteAccount(before.get.accountId)
        after <- accountManager.findAccount(account.accountId)
      //  deleteCol <- accountManager.findDeletedAccount(account.accountId)
      } yield {
        (before, deleted1, deleted2, after)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case (Some(t1), Some(t2), None, None) => 
          t1 must_== t2
          //t1 must_== t4
      }
    }
  }

  object Counter {
    val cnt = new java.util.concurrent.atomic.AtomicLong
  }

  class AccountManager extends After {
    val defaultActorSystem = ActorSystem("AccountManagerTest")
    implicit val execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)

    val mongo = new MockMongo
    val accountManager = new MongoAccountManager(mongo, mongo.database("test_v1"), MongoAccountManagerSettings.defaults)

    val to = Duration(30, "seconds")
  
    val notFoundAccountID = "NOT-GOING-TO-FIND"

    val account = new Account(accountManager.newUserID,
                                        "email",
                                        "test password",
                                        new DateTime(),
                                        "test API key",
                                        "/root",
                                        new AccountPlan(AccountPlan.FreePlan))
    
    Await.result(accountManager.newAccount(account), to)
    
    
    def after = { 
      defaultActorSystem.shutdown 
    }
  }
}
*/
