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
package com.precog.gjallerhorn

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import dispatch._
import org.specs2.mutable._
import scalaz._
import specs2._

class AccountsTask(settings: Settings) extends Task(settings: Settings) with Specification {
  private val DateTimePattern = """[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z""".r

  "accounts web service" should {
    "create account" in {
      val (user, pass) = generateUserAndPassword

      val body = """{ "email": "%s", "password": "%s" }""".format(user, pass)
      val json = getjson((accounts / "") << body)

      (json \ "accountId") must beLike { case JString(_) => ok }
    }

    "not create the same account twice" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val body = """{ "email": "%s", "password": "%s" }""".format(user, pass + "xyz")
      val post = (accounts / "") << body
      val result = Http(post OK as.String)

      val req = (accounts / accountId).as(user, pass + "xyz")
      Http(req > (_.getStatusCode))() must_== 401
    }

    "describe account" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val json = getjson((accounts / accountId).as(user, pass))
      (json \ "accountCreationDate") must beLike { case JString(DateTimePattern()) => ok }
      (json \ "email") must_== JString(user)
      (json \ "accountId") must_== JString(accountId)
      (json \ "apiKey") must_== JString(apiKey)
      (json \ "rootPath") must_== JString(rootPath)
      (json \ "plan") must_== JObject(Map("type" -> JString("Free")))
    }

    "describe account fails for non-owners" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val bad1 = (accounts / accountId).as(user + "zzz", pass)
      Http(bad1 > (_.getStatusCode))() must_== 401

      val bad2 = (accounts / accountId).as(user, pass + "zzz")
      Http(bad2 > (_.getStatusCode))() must_== 401

      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount
      val bad3 = (accounts / accountId).as(user2, pass2)
      Http(bad3 > (_.getStatusCode))() must_== 401
    }

    "add grant to an account" in {
      val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount

      val p = rootPath1 + text(3) + "/"
      val g = createGrant(apiKey1, ("read", p, accountId1 :: Nil) :: Nil).jvalue
      val grantId = (g \ "grantId").deserialize[String]

      val body = JObject("grantId" -> JString(grantId)).renderCompact
      val req = (accounts / accountId2 / "grants" / "").POST.as(user1, pass1) << body
      val r = http(req)().complete()
      listGrantsFor(apiKey2, authApiKey = apiKey1).jvalue.children must contain(g)
    }

    // "describe account's plan" in {}

    // "change account's plan" in {}

    // "change account's password" in {}

    // "delete account's plan" in {}
  }
}

object RunAccounts extends Runner {
  def tasks(settings: Settings) = new AccountsTask(settings) :: Nil
}
