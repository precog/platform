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
    case class PartialSettings(
      host: Option[String] = None,
      id: Option[String] = None, token: Option[String] = None,
      accountsPort: Option[Int] = None, authPort: Option[Int] = None,
      ingestPort: Option[Int] = None, jobsPort: Option[Int] = None,
      shardPort: Option[Int] = None) {

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
    ps.settings.getOrElse(sys.error("missing settings in %s: %s" format (f, ps)))
  }
}

case class Settings(host: String, id: String, token: String, accountsPort: Int, authPort: Int, ingestPort: Int, jobsPort: Int, shardPort: Int)

case class Account(user: String, password: String, accountId: String, apiKey: String, rootPath: String)

abstract class Task(settings: Settings) {
  val Settings(serviceHost, id, token, accountsPort, authPort, ingestPort, jobsPort, shardPort) = settings

  def text(n: Int) = scala.util.Random.alphanumeric.take(12).mkString

  def generateUserAndPassword = (text(12) + "@plastic-idolatry.com", text(12))

  def accounts = host(serviceHost, accountsPort) / "accounts"

  def security = host(serviceHost, authPort) / "apikeys"

  def createAccount: Account = {
    val (user, pass) = generateUserAndPassword

    val post = (accounts / "") << """{ "email": "%s", "password": "%s" }""".format(user, pass)

    val req = Http(post OK as.String)
    val json = JParser.parseFromString(req()).valueOr(throw _)
    val accountId = (json \ "accountId").deserialize[String]

    val xyz = (accounts / accountId).as(user, pass)
    val result: Promise[String] = Http(xyz OK as.String)
    val json2 = JParser.parseFromString(result()).valueOr(throw _)
    val apiKey = (json2 \ "apiKey").deserialize[String]
    val rootPath = (json2 \ "rootPath").deserialize[String]

    Account(user, pass, accountId, apiKey, rootPath)
  }
}

class AccountsTask(settings: Settings) extends Task(settings: Settings) with Specification {
  "accounts web service" should {
    "create accounts" in {
      val (user, pass) = generateUserAndPassword

      val post = (accounts / "") << """{ "email": "%s", "password": "%s" }""".format(user, pass)
      val result = Http(post OK as.String)
      val json = JParser.parseFromString(result())

      json must beLike {
        case Success(jobj) => (jobj \ "accountId") must beLike { case JString(_) => ok }
      }
    }
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

  def deleteAPIKey(authAPIKey: String, apiKey: String) {
    val req = (security / apiKey).DELETE.addQueryParameter("apiKey", authAPIKey)
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

    "delete owned API keys" in {
      val account = createAccount
      val k1 = deriveAPIKey(account)
      val k2 = deriveAPIKey(account)
      val k3 = deriveAPIKey(account)

      deleteAPIKey(account.apiKey, k2)

      val req = (security / "").addQueryParameter("apiKey", account.apiKey)
      val res = Http(req OK as.String)
      val json = JParser.parseFromString(res()).valueOr(throw _)
      val apiKeys = json.children map { obj => (obj \ "apiKey").deserialize[String] }
      apiKeys must haveTheSameElementsAs(List(k1, k3))
    }

    "forbid deletion of authorizing API key" in {
      val account = createAccount
      val req = (security / account.apiKey).DELETE.addQueryParameter("apiKey", account.apiKey)
      val res = Http(req > (_.getStatusCode))
      res() must_== 403 // Forbidden.

      deriveAPIKey(account) must not(throwA[Throwable])
    }

    "forbid deletion of unrelated API key" in {
      val account1 = createAccount
      val childOf1 = deriveAPIKey(account1)

      val account2 = createAccount

      deleteAPIKey(account2.apiKey, childOf1) must throwA[StatusCode]
      deleteAPIKey(account2.apiKey, account1.apiKey) must throwA[StatusCode]

      deriveAPIKey(account1) must not(throwA[StatusCode])
    }

    "forbid deletion of parent API key" in {
      val account1 = createAccount
      val childOf1 = deriveAPIKey(account1)

      deleteAPIKey(childOf1, account1.apiKey) must throwA[StatusCode]
    }
  }
}

object RunAll {
  def main(args: Array[String]) {
    val settings = Settings.fromFile(new java.io.File("shard.out"))
    run(
      new AccountsTask(settings),
      new SecurityTask(settings)
    )
  }
}
