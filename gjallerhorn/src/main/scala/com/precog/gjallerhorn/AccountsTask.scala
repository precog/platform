package com.precog.gjallerhorn

import blueeyes.json._
import dispatch._
import org.specs2.mutable._
import scalaz._
import specs2._

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

object RunAccounts {
  def main(args: Array[String]) {
    try {
    val settings = Settings.fromFile(new java.io.File("shard.out"))
      run(
        new AccountsTask(settings)
      )
    } finally {
      Http.shutdown()
    }
  }
}
