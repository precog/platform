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
