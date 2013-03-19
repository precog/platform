package com.precog.gjallerhorn

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import dispatch._
import org.specs2.mutable._
import specs2._

class SecurityTask(settings: Settings) extends Task(settings: Settings) with Specification {
  "security web service" should {
//     "create derivative apikeys" in {
//       val Account(user, pass, accountId, apiKey, rootPath) = createAccount

//       val req = (security / "").addQueryParameter("apiKey", apiKey) << ("""
// {"name":"MH Test Write",
//  "description":"Foo",
//  "grants":[
//   {"name":"nameWrite",
//    "description":"descWrite",
//    "permissions":[
//     {"accessType":"write",
//      "path":"%sfoo/",
//      "ownerAccountIds":["%s"]}]}]}
// """ format (rootPath, accountId))

//       val result = Http(req OK as.String)
//       val json = JParser.parseFromString(result()).valueOr(throw _)

//       (json \ "name").deserialize[String] must_== "MH Test Write"
//       (json \ "description").deserialize[String] must_== "Foo"
//       (json \ "apiKey").deserialize[String] must_!= apiKey
//       val perms = (json \ "grants").children.flatMap(o => (o \ "permissions").children)
//       perms.map(_ \ "accessType") must_== List(JString("write"))
//       perms.map(_ \ "path") must_== List(JString("%sfoo/" format rootPath))
//     }

//     "list API keys" in {
//       val account = createAccount
//       val k1 = deriveAPIKey(account)
//       val k2 = deriveAPIKey(account)
//       val k3 = deriveAPIKey(account)
//       val req = (security / "").addQueryParameter("apiKey", account.apiKey)
//       val res = Http(req OK as.String)
//       val json = JParser.parseFromString(res()).valueOr(throw _)
//       val apiKeys = json.children map { obj => (obj \ "apiKey").deserialize[String] }
//       apiKeys must haveTheSameElementsAs(List(k1, k2, k3))
//     }

//     "delete API key" in {
//       val account = createAccount

//       deleteAPIKey(account.apiKey) must not(throwA[StatusCode])
//       deriveAPIKey(account) must throwA[Exception]
//     }

//     "deleted child API keys are removed from API key listing" in {
//       val account = createAccount
//       val k1 = deriveAPIKey(account)
//       val k2 = deriveAPIKey(account)
//       val k3 = deriveAPIKey(account)

//       deleteAPIKey(k2)

//       val req = (security / "").addQueryParameter("apiKey", account.apiKey)
//       val res = Http(req OK as.String)
//       val json = JParser.parseFromString(res()).valueOr(throw _)
//       val apiKeys = json.children map { obj => (obj \ "apiKey").deserialize[String] }
//       apiKeys must haveTheSameElementsAs(List(k1, k3))
//     }

//     "list grants" in {
//       val Account(user, pass, accountId, apiKey, rootPath) = createAccount
//       listGrantsFor(apiKey, authApiKey = apiKey) must beLike {
//         case JArray(List(obj)) =>
//           val perms = (obj \ "permissions").children.map { o =>
//             (o \ "path", o \ "ownerAccountIds", o \ "accessType")
//           }.toSet

//           perms must_== Set(
//             (JString("/"),JArray(List(JString(accountId))),JString("read")),
//             (JString("/"),JArray(List(JString(accountId))),JString("reduce")),
//             (JString(rootPath),JArray(Nil),JString("write")),
//             (JString(rootPath),JArray(Nil),JString("delete"))
//           )
//       }
//     }

    "create a new grant" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val subPath = rootPath + "qux/"
      val g = createGrant(apiKey, ("read", subPath, accountId :: Nil) :: Nil)

      val p = JObject(
        "schemaVersion" -> JString("1.0"),
        "path" -> JString(rootPath + "qux/"),
        "accessType" -> JString("read"),
        "ownerAccountIds" -> JArray(JString(accountId) :: Nil))

      val grantId = (g \ "grantId").deserialize[String]
      //TODO: createdAt num => string serialization fix
      //(g \ "createdAt") must beLike { case DateRe(_) => ok }
      (g \ "permissions") must_== JArray(p :: Nil)
    }

    "creating multiple grants works" in {
      val Account(user, pass, accountId, apiKey, rootPath) = createAccount

      val g1 = createGrant(apiKey, ("read", rootPath + "bar/", accountId :: Nil) :: Nil)
      val g2 = createGrant(apiKey, ("read", rootPath + "bar/", accountId :: Nil) :: Nil)

      (g1 \ "grantId") must_!= (g2 \ "grantId")
      (g1 \ "permissions") must_== (g2 \ "permissions")
    }

  }
}

object RunSecurity extends Runner {
  def tasks(settings: Settings) = new SecurityTask(settings) :: Nil
}
