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

case class Account(user: String, password: String, accountId: String, apiKey: String, rootPath: String) {
  def bareRootPath = rootPath.substring(1, rootPath.length - 1)
}

abstract class Task(settings: Settings) {
  val Settings(serviceHost, id, token, accountsPort, authPort, ingestPort, jobsPort, shardPort) = settings

  def text(n: Int) = scala.util.Random.alphanumeric.take(12).mkString

  def generateUserAndPassword = (text(12) + "@plastic-idolatry.com", text(12))

  def accounts = host(serviceHost, accountsPort) / "accounts"

  def security = host(serviceHost, authPort) / "apikeys"
  
  def metadata = host(serviceHost, shardPort) / "meta"

  def ingest = host(serviceHost, ingestPort)

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

  def ingestFile(account: Account, path: String, file: File, contentType: String) {
    val req = ((ingest / "sync" / "fs" / path).POST
                <:< List("Content-Type" -> contentType)
                <<? List("apiKey" -> account.apiKey,
                        "ownerAccountId" -> account.accountId)
                <<< file)
    Http(req OK as.String)()
  }

  def ingestString(account: Account, data: String, contentType: String)(f: Req => Req) {
    val req = (f(ingest / "sync" / "fs").POST
                <:< List("Content-Type" -> contentType)
                <<? List("apiKey" -> account.apiKey,
                        "ownerAccountId" -> account.accountId)
                << data)

    Http(req OK as.String)()
  }

  def ingestString(authAPIKey: String, ownerAccount: Account, data: String, contentType: String)(f: Req => Req) = {
    val req = (f(ingest / "sync" / "fs").POST
                <:< List("Content-Type" -> contentType)
                <<? List("apiKey" -> authAPIKey,
                        "ownerAccountId" -> ownerAccount.accountId)
                << data)

    Http(req OK as.String).either()
  }

  def deletePath(auth: String)(f: Req => Req) {
    val req = f(ingest / "sync" / "fs").DELETE <<? List("apiKey" -> auth)
    Http(req OK as.String)()
  }

  def metadataFor(apiKey: String, tpe: Option[String] = None, prop: Option[String] = None)(f: Req => Req): JValue = {
    val params = List(
      Some("apiKey" -> apiKey),
      tpe map ("type" -> _),
      prop map ("property" -> _)
    ).flatten
    val req = f(metadata / "fs") <<? params
    val res = Http(req OK as.String)
    val json = JParser.parseFromString(res()).valueOr(throw _)
    json
  }
}

object RunAll {
  def main(args: Array[String]) {
    try {
    val settings = Settings.fromFile(new java.io.File("shard.out"))
      run(
        new AccountsTask(settings),
        new SecurityTask(settings),
        new MetadataTask(settings),
        new ScenariosTask(settings)
      )
    } finally {
      Http.shutdown()
    }
  }
}
