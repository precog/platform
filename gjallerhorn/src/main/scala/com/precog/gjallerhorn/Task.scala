package com.precog.gjallerhorn

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import com.ning.http.client.RequestBuilder
import dispatch._
import java.io._
import org.specs2.mutable.Specification
import specs2._

abstract class Task(settings: Settings) extends Specification {
  val Settings(serviceHost, id, token, accountsPort, authPort, ingestPort, jobsPort, shardPort) = settings

  def http(rb: RequestBuilder): Promise[ApiResult] =
    Http(rb).map { r =>
      val s = r.getResponseBody
      r.getStatusCode match {
        case 200 => JParser.parseFromString(s).fold(ApiBadJson, ApiResponse)
        case n => ApiFailure(n, s)
      }
    }

  def text(n: Int) = scala.util.Random.alphanumeric.take(12).mkString

  def generateUserAndPassword = (text(12) + "@plastic-idolatry.com", text(12))

  def accounts = host(serviceHost, accountsPort) / "accounts"

  def security = host(serviceHost, authPort) / "apikeys"

  def grants = host(serviceHost, authPort) / "grants"
  
  def metadata = host(serviceHost, shardPort) / "meta"

  def ingest = host(serviceHost, ingestPort)

  def analytics = host(serviceHost, shardPort) / "analytics"

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

  def listGrantsFor(targetApiKey: String, authApiKey: String): JValue = {
    val req = (security / targetApiKey / "grants" / "").addQueryParameter("apiKey", authApiKey)
    val result = Http(req OK as.String)
    JParser.parseFromString(result()).valueOr(throw _)
  }

  def createGrant(apiKey: String, perms: List[(String, String, List[String])]): JValue = {
    val body = JObject(
      "permissions" -> JArray(
        perms.map { case (accessType, path, owners) =>
            val ids = JArray(owners.map(JString(_)))
            JObject(
              "accessType" -> JString(accessType),
              "path" -> JString(path),
              "ownerAccountIds" -> ids)
        })).renderCompact

    http((grants / "").POST.addQueryParameter("apiKey", apiKey) << body)().jvalue
  }
}
