package com.precog.gjallerhorn

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import dispatch._

import org.specs2.mutable._
import org.specs2.execute.EventuallyResults
import specs2._

import scalaz._

class ScenariosTask(settings: Settings) extends Task(settings: Settings) with Specification {
  val dummyEvents = """
{ "data": "Hello world" }
{ "data": "Goodbye cruel world" }
"""
  
  "aggregated services" should {
    "create account, ingest data, query" in {
      val account = createAccount
      val res = ingestString(account, dummyEvents, "application/json")(_ / account.bareRootPath / "foo")

      EventuallyResults.eventually(10, 1.second) {
        val res = (analytics / "fs" / account.bareRootPath) <<? List("apiKey" -> account.apiKey, "q" -> "count(//foo)")
        val str = Http(res OK as.String)()
        val json = JParser.parseFromString(Http(res OK as.String)()).valueOr(throw _)
        val count = json(0).deserialize[Double]
        count must_== 2
      }
    }

    "support ingesting under own account root" in {
      val account = createAccount
      
      val res = ingestString(account.apiKey, account, dummyEvents, "application/json")(_ / account.bareRootPath / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }
      
      res must beLike {
        case Success(jobj) => ok
      }
    }
    
    "support ingesting under own account root with other owner" in {
      val account1 = createAccount
      val account2 = createAccount
      
      val res = ingestString(account1.apiKey, account2, dummyEvents, "application/json")(_ / account1.bareRootPath / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }
      
      res must beLike {
        case Success(jobj) => ok
      }
    }
    
    "support browsing of own data under own account root" in {
      val account = createAccount
      
      val res = ingestString(account.apiKey, account, dummyEvents, "application/json")(_ / account.bareRootPath / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }
      
      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo")
        (json \ "size").deserialize[Long] must_== 2
      }
    }
    
    "prevent ingesting under an unauthorized path" in {
      val account = createAccount
      
      val res = ingestString(account.apiKey, account, dummyEvents, "application/json")(_ / ("not-"+account.bareRootPath) / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }

      res must beLike {
        case Failure(StatusCode(403)) => ok
      }
    }
    
    "support browsing of own data under other account root" in {
      val account1 = createAccount
      val account2 = createAccount
      
      val res = ingestString(account1.apiKey, account2, dummyEvents, "application/json")(_ / account1.bareRootPath / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }
      
      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account2.apiKey)(_ / account1.bareRootPath / "foo")
        (json \ "size").deserialize[Long] must_== 2
      }
    }
    
    "prevent browsing of other data under own account root" in {
      val account1 = createAccount
      val account2 = createAccount
      
      val res = ingestString(account1.apiKey, account2, dummyEvents, "application/json")(_ / account1.bareRootPath / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }
      
      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account1.apiKey)(_ / account1.bareRootPath / "foo")
        (json \ "size").deserialize[Long] must_== 0
      }
    }
    
    "support delegation of ability to write as self to own root to others" in {
      val account1 = createAccount
      val account2 = createAccount
      
      val req = (security / "").addQueryParameter("apiKey", account1.apiKey) <<
        ("""{"grants":[{"permissions":[{"accessType":"write", "path":"%s", "ownerAccountIds":["%s"]}]}]}""" format
          (account1.rootPath+"/foo", account1.accountId))

      val result = Http(req OK as.String)
      val json = JParser.parseFromString(result()).valueOr(throw _)

      val delegateAPIKey = (json \ "apiKey").deserialize[String]
      delegateAPIKey must_!= account1.apiKey
      delegateAPIKey must_!= account2.apiKey
      
      val res = ingestString(delegateAPIKey, account1, dummyEvents, "application/json")(_ / account1.bareRootPath / "foo") match {
        case Left(thr) => Failure(thr)
        case Right(s) => JParser.parseFromString(s)
      }
      
      res must beLike {
        case Success(jobj) => ok
      }
      
      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account1.apiKey)(_ / account1.bareRootPath / "foo")
        (json \ "size").deserialize[Long] must_== 2
      }
    }
  }
}

object RunScenarios extends Runner {
  def tasks(settings: Settings) = new ScenariosTask(settings) :: Nil
}

