package com.precog.gjallerhorn

import blueeyes.json._
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

    // "add grant to an account" in {
    //   val Account(user1, pass1, accountId1, apiKey1, rootPath1) = createAccount
    //   val Account(user2, pass2, accountId2, apiKey2, rootPath2) = createAccount
    //   //val Account(user3, pass3, accountId3, apiKey3, rootPath3) = createAccount

    //   def grant(id: String, u: String, p: String, n: Int) =
    //     (accounts / id / "grants" / "").as(u, p) << ("[" + n + "]")

    //   val h1 = Http(grant(accountId2, user1, pass1, 6) OK as.String)
    //   println(h1())
    // }

    // "describe account's plan" in {}

    // "change account's plan" in {}

    // "change account's password" in {}

    // "delete account's plan" in {}
  }
}

object RunAccounts extends Runner {
  def tasks(settings: Settings) = new AccountsTask(settings) :: Nil
}
