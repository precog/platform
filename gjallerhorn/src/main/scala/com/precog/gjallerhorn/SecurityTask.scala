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
import specs2._

class SecurityTask(settings: Settings) extends Task(settings: Settings) with Specification {
  "security web service" should {
    "create derivative apikeys" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val req = (security / "").addQueryParameter("apiKey", apiKey) << ("""
{"name":"MH Test Write",
 "description":"Foo",
 "grants":[
  {"name":"nameWrite",
   "description":"descWrite",
   "permissions":[
    {"accessType":"write",
     "path":"%sfoo/",
     "ownerAccountIds":["%s"]}]}]}
""" format (rootPath, accountId))

      val result = Http(req OK as.String)
      val json = JParser.parseFromString(result()).valueOr(throw _)

      (json \ "name").deserialize[String] must_== "MH Test Write"
      (json \ "description").deserialize[String] must_== "Foo"
      (json \ "apiKey").deserialize[String] must_!= apiKey
      val perms = (json \ "grants").children.flatMap(o => (o \ "permissions").children)
      perms.map(_ \ "accessType") must_== List(JString("write"))
      perms.map(_ \ "path") must_== List(JString("%sfoo/" format rootPath))
    }

    "list API keys" in {
      val account = createAccount
      val k1 = deriveAPIKey(account)
      val k2 = deriveAPIKey(account)
      val k3 = deriveAPIKey(account)
      val req = (security / "").addQueryParameter("apiKey", account.apiKey)
      val res = Http(req OK as.String)
      val json = JParser.parseFromString(res()).valueOr(throw _)
      val apiKeys = json.children map { obj => (obj \ "apiKey").deserialize[String] }
      apiKeys must haveTheSameElementsAs(List(k1, k2, k3))
    }

    "delete API key" in {
      val account = createAccount

      deleteAPIKey(account.apiKey) must not(throwA[StatusCode])
      deriveAPIKey(account) must throwA[Exception]
    }

    "deleted child API keys are removed from API key listing" in {
      val account = createAccount
      val k1 = deriveAPIKey(account)
      val k2 = deriveAPIKey(account)
      val k3 = deriveAPIKey(account)

      deleteAPIKey(k2)

      val req = (security / "").addQueryParameter("apiKey", account.apiKey)
      val res = Http(req OK as.String)
      val json = JParser.parseFromString(res()).valueOr(throw _)
      val apiKeys = json.children map { obj => (obj \ "apiKey").deserialize[String] }
      apiKeys must haveTheSameElementsAs(List(k1, k3))
    }

    "list grants" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount
      listGrantsFor(apiKey, authApiKey = apiKey).jvalue must beLike {
        case JArray(List(obj)) =>
          val perms = (obj \ "permissions").children.map { o =>
            (o \ "path", o \ "ownerAccountIds", o \ "accessType")
          }.toSet

          perms must_== Set(
            (JString("/"),JArray(List(JString(accountId))),JString("read")),
            //(JString("/"),JArray(List(JString(accountId))),JString("reduce")),
            (JString(rootPath),JArray(Nil),JString("write")),
            (JString(rootPath),JArray(Nil),JString("delete"))
          )
      }
    }

    "create a new grant" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val subPath = rootPath + "qux/"
      val g = createGrant(apiKey, ("read", subPath, accountId :: Nil) :: Nil).jvalue

      val p = JObject(
        "schemaVersion" -> JString("1.0"),
        "path" -> JString(rootPath + "qux/"),
        "accessType" -> JString("read"),
        "ownerAccountIds" -> JArray(JString(accountId) :: Nil))

      val grantId = (g \ "grantId").deserialize[String]
      (g \ "permissions") must_== JArray(p :: Nil)
    }

    "creating multiple grants works" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val p = rootPath + text(3) + "/" + text(4) + "/"
      val g1 = createGrant(apiKey, ("read", p, accountId :: Nil) :: Nil).jvalue
      val g2 = createGrant(apiKey, ("read", p, accountId :: Nil) :: Nil).jvalue

      (g1 \ "grantId") must_!= (g2 \ "grantId")
      (g1 \ "permissions") must_== (g2 \ "permissions")
    }

    "add a grant to an api key" in {
      val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount

      val p = rootPath1 + text(12) + "/"
      val g = createGrant(apiKey1, ("read", p, accountId1 :: Nil) :: Nil).jvalue
      val grantId = (g \ "grantId").deserialize[String]

      listGrantsFor(apiKey2, authApiKey = apiKey1).jvalue.children must not(contain(g))
      addToGrant(apiKey2, apiKey1, grantId).complete()
      listGrantsFor(apiKey2, authApiKey = apiKey1).jvalue.children must contain(g)
    }

    "remove a grant from an api key" in {
      val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount

      val p = rootPath1 + text(3) + "/" + text(3) + "/" + text(3)
      val g = createGrant(apiKey1, ("read", p, accountId1 :: Nil) :: Nil).jvalue
      val grantId = (g \ "grantId").deserialize[String]

      listGrantsFor(apiKey2, authApiKey = apiKey1).jvalue.children must not(contain(g))
      addToGrant(apiKey2, apiKey1, grantId).complete()
      listGrantsFor(apiKey2, authApiKey = apiKey1).jvalue.children must contain(g)
      removeGrant(apiKey2, apiKey1, grantId).complete()
      listGrantsFor(apiKey2, authApiKey = apiKey1).jvalue.children must not(contain(g))
    }

    "describe a grant" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount
      val p = rootPath + text(3) + "/" + text(3) + "/" + text(3)
      val g = createGrant(apiKey, ("read", p, accountId :: Nil) :: Nil).jvalue
      val gg = describeGrant(apiKey, (g \ "grantId").deserialize[String]).jvalue
      g must_== gg
    }

    "not describe invalid grants" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount
      describeGrant(apiKey, "does not exist") must beLike {
        case ApiFailure(404, "\"Unable to find grant does not exist\"") => ok 
      }
    }

    "create child grants" in {
      val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount

      val p = rootPath1 + text(3) + "/"
      val g = createGrant(apiKey1, ("read", p, accountId1 :: Nil) :: Nil).jvalue
      val grantId = (g \ "grantId").deserialize[String]
      addToGrant(apiKey2, apiKey1, grantId).complete()

      val p2 = p + text(4) + "/"
      val child = createChildGrant(apiKey2, grantId, ("read", p2, accountId1 :: Nil) :: Nil).jvalue
      val childId = (child \ "grantId").deserialize[String]

      describeGrant(apiKey1, childId).jvalue must_== child
    }

    "prevent illegal child grants #1" in {
      val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount

      val p = rootPath1 + text(3) + "/"
      val g = createGrant(apiKey1, ("read", p, accountId1 :: Nil) :: Nil).jvalue
      val grantId = (g \ "grantId").deserialize[String]

      val p2 = p + text(4) + "/"
      createChildGrant(apiKey2, grantId, ("read", p2, accountId1 :: Nil) :: Nil) must beLike {
        case ApiFailure(400, "{\"error\":\"Requestor lacks permissions to create grant.\"}") => ok
      }
    }

    "prevent illegal child grants #2" in {
      val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
      val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount

      val p = rootPath1 + text(3) + "/"
      val g = createGrant(apiKey1, ("read", p, accountId1 :: Nil) :: Nil).jvalue
      val grantId = (g \ "grantId").deserialize[String]
      addToGrant(apiKey2, apiKey1, grantId).complete()

      val p2 = p + text(4) + "/"
      createChildGrant(apiKey2, grantId, ("read", p2, accountId2 :: Nil) :: Nil) must beLike {
        case ApiFailure(400, "{\"error\":\"Requestor lacks permissions to create grant.\"}") => ok
      }
    }
  }
}

object RunSecurity extends Runner {
  def tasks(settings: Settings) = new SecurityTask(settings) :: Nil
}
