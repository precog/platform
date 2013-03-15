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
  }
}

object RunSecurity {
  def main(args: Array[String]) {
    try {
    val settings = Settings.fromFile(new java.io.File("shard.out"))
      run(
        new SecurityTask(settings)
      )
    } finally {
      Http.shutdown()
    }
  }
}
