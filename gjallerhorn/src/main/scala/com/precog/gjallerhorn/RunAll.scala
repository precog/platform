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
import com.ning.http.client.RequestBuilder
import dispatch._
import java.io._
import org.specs2.mutable._
import scalaz._
import specs2._

object Settings {
  private val IdRegex = """^id (.+)$""".r
  private val TokenRegex = """^token ([-A-F0-9]+)$""".r
  private val AccountsPort = """^accounts (\d+)$""".r
  private val AuthPort = """^auth (\d+)$""".r
  private val IngestPort = """^ingest (\d+)$""".r
  private val JobsPort = """^jobs (\d+)$""".r
  private val ShardPort = """^shard (\d+)$""".r

  def fromFile(f: File): Settings = {
    if (!f.canRead) sys.error("Can't read %s. Is shard running?" format f)

    case class PartialSettings(
      host: Option[String] = None,
      id: Option[String] = None, token: Option[String] = None,
      accountsPort: Option[Int] = None, authPort: Option[Int] = None,
      ingestPort: Option[Int] = None, jobsPort: Option[Int] = None,
      shardPort: Option[Int] = None) {

      def missing: List[String] = {
        def q(o: Option[_], s: String): List[String] =
          if (o.isDefined) Nil else s :: Nil

        q(host, "host") ++ q(id, "id") ++ q(token, "token") ++
        q(accountsPort, "accountsPort") ++ q(authPort, "authPort") ++
        q(ingestPort, "ingestPort") ++ q(jobsPort, "jobsPort") ++
        q(shardPort, "shardPort")
      }

      def settings: Option[Settings] = for {
        h <- host
        i <- id
        t <- token
        ac <- accountsPort
        au <- authPort
        in <- ingestPort
        j <- jobsPort
        sh <- shardPort
      } yield {
        Settings(h, i, t, ac, au, in, j, sh)
      }
    }

    val lines = io.Source.fromFile(f).getLines
    val ps = lines.foldLeft(PartialSettings(host = Some("localhost"))) { (ps, s) =>
      s match {
        case IdRegex(s) => ps.copy(id = Some(s))
        case TokenRegex(s) => ps.copy(token = Some(s))
        case AccountsPort(n) => ps.copy(accountsPort = Some(n.toInt))
        case AuthPort(n) => ps.copy(authPort = Some(n.toInt))
        case IngestPort(n) => ps.copy(ingestPort = Some(n.toInt))
        case JobsPort(n) => ps.copy(jobsPort = Some(n.toInt))
        case ShardPort(n) => ps.copy(shardPort = Some(n.toInt))
        case _ => ps
      }
    }
    ps.settings.getOrElse {
      sys.error("missing settings in %s:\n  %s" format (f, ps.missing.mkString("\n  ")))
    }
  }
}

case class Settings(host: String, id: String, token: String, accountsPort: Int,
  authPort: Int, ingestPort: Int, jobsPort: Int, shardPort: Int)

case class Account(user: String, password: String, accountId: String, apiKey: String, rootPath: String)

abstract class Task(settings: Settings) {
  val Settings(serviceHost, id, token, accountsPort, authPort, ingestPort, jobsPort, shardPort) = settings

  def text(n: Int) = scala.util.Random.alphanumeric.take(12).mkString

  def generateUserAndPassword = (text(12) + "@plastic-idolatry.com", text(12))

  def accounts = host(serviceHost, accountsPort) / "accounts"

  def security = host(serviceHost, authPort) / "apikeys"

  def getjson(rb: RequestBuilder) =
    JParser.parseFromString(Http(rb OK as.String)()).valueOr(throw _)

  def createAccount: Account = {
    val (user, pass) = generateUserAndPassword

    val body = """{ "email": "%s", "password": "%s" }""".format(user, pass)
    val json = getjson((accounts / "") << body)
    val accountId = (json \ "accountId").deserialize[String]

    val json2 = getjson((accounts / accountId).as(user, pass))
    val apiKey = (json2 \ "apiKey").deserialize[String]
    val rootPath = (json2 \ "rootPath").deserialize[String]

    Account(user, pass, accountId, apiKey, rootPath)
  }
}

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
    }

    "describe account's plan" in {}

    "change account's plan" in {}

    "change account's password" in {}

    "delete account's plan" in {}
  }
}

class SecurityTask(settings: Settings) extends Task(settings: Settings) with Specification {
  def deriveAPIKey(parent: Account, subPath: String = ""): String = {
    val body = """{"grants":[{"permissions":[{"accessType":"read","path":"%s","ownerAccountIds":["%s"]}]}]}"""
    val req = (security / "").addQueryParameter("apiKey", parent.apiKey) << {
      body.format(parent.rootPath + subPath, parent.accountId)
    }
    val result = Http(req OK as.String)
    val json = JParser.parseFromString(result()).valueOr(throw _)
    (json \ "apiKey").deserialize[String]
  }

  def deleteAPIKey(apiKey: String) {
    val req = (security / apiKey).DELETE
    val res = Http(req OK as.String)
    res()
  }

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
  }
}

object RunAll {
  def main(args: Array[String]) {
    val settings = Settings.fromFile(new File("shard.out"))
    val tasks = (
      new AccountsTask(settings) ::
      //new SecurityTask(settings) ::
      Nil
    )

    run(tasks: _*)
  }
}
