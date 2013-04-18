package com.precog.gjallerhorn

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import dispatch._

import java.io._

import org.specs2.mutable._
import org.specs2.time.TimeConversions._
import org.specs2.execute.EventuallyResults
import specs2._

import scalaz._

class IngestTask(settings: Settings) extends Task(settings: Settings) with Specification {

  val simpleData = """
    {"a":1,"b":"Tom"}
    {"a":2,"b":3}
    {"a":3,"b":true}
    {"a":4,"b":null}
    {"a":5,"c":"asdf"}
  """

  "ingest" should {
     "ingest json without a content type" in {
       val account = createAccount
       val req = (((ingest / "sync" / "fs").POST / account.bareRootPath / "foo" / "")
                   <<? List("apiKey" -> account.apiKey,
                            "ownerAccountId" -> account.accountId)
                   << simpleData)
       val res = http(req)()
       EventuallyResults.eventually(10, 1.second) {
         val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
         (json \ "size").deserialize[Long] must_== 5
       }
     }

    "ingest multiple sync requests" in {
      val account = createAccount
      (1 to 20) foreach { _ =>
        ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo")
      }
      EventuallyResults.eventually(20, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo")
        (json \ "size").deserialize[Long] must_== 100
      }
    }

    "ingest multiple async requests" in {
      val account = createAccount
      (1 to 20) foreach { _ =>
        asyncIngestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "")
      }
    
      EventuallyResults.eventually(20, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
        (json \ "size").deserialize[Long] must_== 100
      }
    }
    
    val csvData = """a,b,c,d
                    |1.2,asdf,,
                    |1e-308,"a,b,c",33,43
                    |-1e308,hello world,x,2""".stripMargin
    
    val ssvData = """a;b;c;d
                    |1.2;asdf;;
                    |1e-308;"a,b,c";33;43
                    |-1e308;hello world;x;2""".stripMargin
    
    val tsvData = """a    b    c    d
                    |1.2    asdf        
                    |1e-308    "a,b,c"    33    43
                    |-1e308    hello world    x    2""".stripMargin
    
    val expected = JParser.parseFromString("""[
      { "a": 1.2, "b": "asdf", "c": null, "d": null },
      { "a": 1e-308, "b": "a,b,c", "c": "33", "d": 43 },
      { "a": -1e308, "b": "hello world", "c": "x", "d": 2 }
    ]""").valueOr(throw _).children

    //FIXME
    // csv can no longer be ingested this way
    
    //"ingest CSV synchronously" in {
    //  val account = createAccount
    //  ingestString(account, csvData, "text/csv")(_ / account.bareRootPath / "foo" / "")
    //
    //  EventuallyResults.eventually(10, 1.second) {
    //    val url = analytics / "fs" / account.bareRootPath / ""
    //    val req = url <<? List("apiKey" -> account.apiKey, "q" -> "//foo")
    //    val json = JParser.parseFromString(Http(req OK as.String)()).valueOr(throw _)
    //    val data = json.children
    //    data must_== expected
    //  }
    //}
    //
    //"ingest SSV synchronously" in {
    //  val account = createAccount
    //  ingestString(account, ssvData, "text/csv") { req =>
    //    (req / account.bareRootPath / "foo" / "") <<? List("delimiter" -> ";")
    //  }
    //
    //  EventuallyResults.eventually(10, 1.second) {
    //    val url = analytics / "fs" / account.bareRootPath / ""
    //    val req = url <<? List("apiKey" -> account.apiKey, "q" -> "//foo")
    //    val json = JParser.parseFromString(Http(req OK as.String)()).valueOr(throw _)
    //    val data = json.children
    //    data must_== expected
    //  }
    //}
    //
    //"ingest TSV synchronously" in {
    //  val account = createAccount
    //  ingestString(account, tsvData, "text/csv") { req =>
    //    (req / account.bareRootPath / "foo" / "") <<? List("delimiter" -> "    ")
    //  }
    //
    //  EventuallyResults.eventually(10, 1.second) {
    //    val url = analytics / "fs" / account.bareRootPath / ""
    //    val req = url <<? List("apiKey" -> account.apiKey, "q" -> "//foo")
    //    val json = JParser.parseFromString(Http(req OK as.String)()).valueOr(throw _)
    //    val data = json.children
    //    data must_== expected
    //  }
    //}
  }
}

object RunIngest extends Runner {
  def tasks(settings: Settings) = new IngestTask(settings) :: Nil
}
