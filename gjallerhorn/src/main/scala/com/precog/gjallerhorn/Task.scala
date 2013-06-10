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
import org.specs2.mutable.Specification
import specs2._

abstract class Task(settings: Settings) extends Specification {
  val Settings(serviceHost, id, token, accountsPort, accountsPath, authPort, authPath, ingestPort, ingestPath, jobsPort, jobsPath, shardPort, shardPath, secure) = settings

  def http(rb: RequestBuilder): Promise[ApiResult] =
    Http(rb).map { r =>
      val s = r.getResponseBody
      r.getStatusCode match {
        case 200 =>
          JParser.parseFromString(s).fold(ApiBadJson, ApiResponse)
        case n if 200 < n && n < 300 =>
          // things like 201 may not have a message body
          if (s.isEmpty)
            ApiResponse(JNull)
          else
            JParser.parseFromString(s).fold(ApiBadJson, ApiResponse)
        case n =>
          ApiFailure(n, s)
      }
    }

  def text(n: Int) = scala.util.Random.alphanumeric.take(12).mkString

  def generateUserAndPassword = (text(12) + "@plastic-idolatry.com", text(12))

  def accounts = host0(serviceHost, accountsPort) / "accounts" / "v1" / "accounts"

  def security = host0(serviceHost, authPort) / "security" / "v1" / "apikeys"

  def grants = host0(serviceHost, authPort) / "security" / "v1" / "grants"
  
  def metadata = host0(serviceHost, shardPort) / "analytics" / "v2" / "meta"

  def ingest = host0(serviceHost, ingestPort) / "ingest" / "v2"

  def analytics = host0(serviceHost, shardPort) / "analytics" / "v2" / "analytics"

  def getjson(rb: RequestBuilder, setContentType: Boolean = true) = {
    val rb2 = if (setContentType) rb <:< List("Content-Type" -> "application/json") else rb
    JParser.parseFromString(Http(rb2 OK as.String)()).valueOr(throw _)
  }

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
                <<? List("apiKey" -> account.apiKey/*,
                        "ownerAccountId" -> account.accountId*/)
                <<< file)
    Http(req OK as.String)()
  }

  def ingestString(account: Account, data: String, contentType: String)(f: Req => Req) {
    val req = (f(ingest / "sync" / "fs").POST
                <:< List("Content-Type" -> contentType)
                <<? List("apiKey" -> account.apiKey/*,
                        "ownerAccountId" -> account.accountId*/)
                << data)
    Http(req OK as.String)()
  }

  def asyncIngestString(account: Account, data: String, contentType: String)(f: Req => Req) {
    val req = (f(ingest / "async" / "fs").POST
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
    Http(req OK as.String).either.apply().fold(
      error => JUndefined,
      json => JParser.parseFromString(json).valueOr(throw _)
    )
  }

  def listGrantsFor(targetApiKey: String, authApiKey: String): ApiResult =
    http((security / targetApiKey / "grants" / "").addQueryParameter("apiKey", authApiKey))()

  def grantBody(perms: List[(String, String, List[String])]): String =
    JObject(
      "permissions" -> JArray(
        perms.map { case (accessType, path, owners) =>
            val ids = JArray(owners.map(JString(_)))
            JObject(
              "accessType" -> JString(accessType),
              "path" -> JString(path),
              "ownerAccountIds" -> ids)
        })).renderCompact

  def createGrant(apiKey: String, perms: List[(String, String, List[String])]): ApiResult =
    http((grants / "").POST.addQueryParameter("apiKey", apiKey) << grantBody(perms))()

  def createChildGrant(apiKey: String, grantId: String, perms: List[(String, String, List[String])]): ApiResult =
    http((grants / grantId / "children" / "").POST.addQueryParameter("apiKey", apiKey) << grantBody(perms))()

  def addToGrant(targetApiKey: String, authApiKey: String, grantId: String): ApiResult = {
    val body = JObject("grantId" -> JString(grantId)).renderCompact
    val url = security / targetApiKey / "grants" / ""
    val req = (url).addQueryParameter("apiKey", authApiKey) << body
    http(req)()
  }

  def removeGrant(targetApiKey: String, authApiKey: String, grantId: String): ApiResult = {
    val url = security / targetApiKey / "grants" / grantId
    val req = (url).DELETE.addQueryParameter("apiKey", authApiKey)
    http(req)()
  }

  def describeGrant(apiKey: String, grantId: String): ApiResult =
    http((grants / grantId).addQueryParameter("apiKey", apiKey))()

  private def host0(domain: String, port: Int) = {
    val back = host(domain, port)
    // port check is a hack to allow just accounts to run over https
    if (secure && port != 80) back.secure else back
  }
}
