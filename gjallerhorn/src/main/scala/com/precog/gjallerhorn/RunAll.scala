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
import specs2._

object Settings {
  private val HostRegex = """^host (.+)$""".r
  private val IdRegex = """^id (.+)$""".r
  private val TokenRegex = """^token ([-A-F0-9]+)$""".r
  private val AccountsPort = """^accounts (\d+)$""".r
  private val AccountsPath = """^accounts\-path (.+)$""".r
  private val AuthPort = """^auth (\d+)$""".r
  private val AuthPath = """^auth\-path (.+)$""".r
  private val IngestPort = """^ingest (\d+)$""".r
  private val IngestPath = """^ingest\-path (.+)$""".r
  private val JobsPort = """^jobs (\d+)$""".r
  private val JobsPath = """^jobs\-path (.+)$""".r
  private val ShardPort = """^shard (\d+)$""".r
  private val ShardPath = """^shard\-path (.+)$""".r
  private val SecureRegex = """^secure (true|false)$""".r

  def fromFile(f: File): Settings = {
    if (!f.canRead) sys.error("Can't read %s. Is shard running?" format f)

    case class PartialSettings(
        host: Option[String] = None,
        id: Option[String] = None,
        token: Option[String] = None,
        accountsPort: Option[Int] = None,
        accountsPath: Option[String] = None,
        authPort: Option[Int] = None,
        authPath: Option[String] = None,
        ingestPort: Option[Int] = None,
        ingestPath: Option[String] = None,
        jobsPort: Option[Int] = None,
        jobsPath: Option[String] = None,
        shardPort: Option[Int] = None,
        shardPath: Option[String] = None,
        secure: Option[Boolean] = None) {

      def missing: List[String] = {
        def q(o: Option[_], s: String): List[String] =
          if (o.isDefined) Nil else s :: Nil

        q(host, "host") ++ q(id, "id") ++ q(token, "token") ++
        q(accountsPort, "accountsPort") ++ q(accountsPath, "accountsPath") ++
        q(authPort, "authPort") ++ q(authPath, "authPath") ++ 
        q(ingestPort, "ingestPort") ++ q(ingestPath, "ingestPath") ++ 
        q(jobsPort, "jobsPort") ++ q(jobsPath, "jobsPath") ++ 
        q(shardPort, "shardPort") ++ q(shardPath, "shardPath") ++ q(secure, "secure")
      }

      def settings: Option[Settings] = for {
        h <- host
        i <- id
        t <- token
        ac <- accountsPort
        acp <- accountsPath
        au <- authPort
        aup <- authPath
        in <- ingestPort
        inp <- ingestPath
        j <- jobsPort
        jp <- jobsPath
        sh <- shardPort
        shp <- shardPath
        sec <- secure
      } yield {
        Settings(h, i, t, ac, acp, au, aup, in, inp, j, jp, sh, shp, sec)
      }
    }

    val lines = io.Source.fromFile(f).getLines
    
    val defaults = PartialSettings(
      host = Some("localhost"),
      accountsPath = Some("accounts"),
      authPath = Some("apikeys"),
      ingestPath = Some("ingest"),
      shardPath = Some("meta"),
      secure = Some(false))
    
    val ps = lines.foldLeft(defaults) { (ps, s) =>
      val ps2 = s match {
        case HostRegex(s) => ps.copy(host = Some(s))
        case IdRegex(s) => ps.copy(id = Some(s))
        case TokenRegex(s) => ps.copy(token = Some(s))
        case AccountsPort(n) => ps.copy(accountsPort = Some(n.toInt))
        case AccountsPath(p) => ps.copy(accountsPath = Some(p))
        case AuthPort(n) => ps.copy(authPort = Some(n.toInt))
        case AuthPath(p) => ps.copy(authPath = Some(p))
        case _ => ps
      }
      
      // split to avoid a bug in the pattern matcher
      s match {
        case IngestPort(n) => ps2.copy(ingestPort = Some(n.toInt))
        case IngestPath(p) => ps2.copy(ingestPath = Some(p))
        case JobsPort(n) => ps2.copy(jobsPort = Some(n.toInt))
        case JobsPath(p) => ps2.copy(jobsPath = Some(p))
        case ShardPort(n) => ps2.copy(shardPort = Some(n.toInt))
        case ShardPath(p) => ps2.copy(shardPath = Some(p))
        case SecureRegex(s) => ps2.copy(secure = Some(s.toBoolean))
        case _ => ps2
      }
    }
    ps.settings.getOrElse {
      sys.error("missing settings in %s:\n  %s" format (f, ps.missing.mkString("\n  ")))
    }
  }
}

case class Settings(host: String, id: String, token: String, accountsPort: Int,
  accountsPath: String, authPort: Int, authPath: String, ingestPort: Int,
  ingestPath: String, jobsPort: Int, jobsPath: String, shardPort: Int,
  shardPath: String, secure: Boolean)

case class Account(user: String, password: String, accountId: String, apiKey: String, rootPath: String) {
  def bareRootPath = rootPath.substring(1, rootPath.length - 1)
}

abstract class Task(settings: Settings) {
  val Settings(serviceHost, id, token, accountsPort, accountsPath, authPort, authPath, ingestPort, ingestPath, jobsPort, jobsPath, shardPort, shardPath, secure) = settings

  def text(n: Int) = scala.util.Random.alphanumeric.take(12).mkString

  def generateUserAndPassword = (text(12) + "@plastic-idolatry.com", text(12))

  def accounts =
    (accountsPath split "/").foldLeft(host0(serviceHost, accountsPort)) { _ / _ }
  
  def security =
    (authPath split "/").foldLeft(host0(serviceHost, authPort)) { _ / _ }
  
  def metadata =
    (shardPath split "/").foldLeft(host0(serviceHost, shardPort)) { _ / _ }

  def ingest =
    (ingestPath split "/").foldLeft(host0(serviceHost, ingestPort)) { _ / _ }

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
  
  private def host0(domain: String, port: Int) = {
    val back = host(domain, port)
    // port check is a hack to allow just accounts to run over https
    if (secure && port != 80) back.secure else back
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
