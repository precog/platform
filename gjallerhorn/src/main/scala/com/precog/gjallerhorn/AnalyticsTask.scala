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

class AnalyticsTask(settings: Settings) extends Task(settings: Settings) with Specification {

  val simpleData = """
    {"a":1,"b":"Tom"}
    {"a":2,"b":3}
    {"a":3,"b":true}
    {"a":4,"b":null}
    {"a":5,"c":"asdf"}
  """

  def asyncQuery(auth: String, prefixPath: String, query: String): String = {
    val req = (analytics / "queries").POST <<? List(
      "apiKey" -> auth,
      "q" -> query,
      "prefixPath" -> prefixPath
    )
    val json = JParser.parseFromString(Http(req OK as.String)()).valueOr(throw _)
    (json \ "jobId").deserialize[String]
  }

  "analytics web service" should {

    // The user ingests some data, waits for it to complete, then wants to
    // run an async query. They should get a job ID back that they can use to
    // retreive the results at some point.
    "run async queries" in {
      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
        (json \ "size").deserialize[Long] must_== 5
      }

      val jobId = asyncQuery(account.apiKey, "/" + account.bareRootPath, "mean((//foo).a)")

      EventuallyResults.eventually(10, 1.second) {
        val res = (analytics / "queries" / jobId) <<? List("apiKey" -> account.apiKey)
        val str = Http(res OK as.String)()
        if (str != "") {
          val json = JParser.parseFromString(Http(res OK as.String)()).valueOr(throw _)
          val mean = (json \ "data")(0).deserialize[Double]
          mean must_== 3.0
        } else {
          str must_!= ""
        }
      }
    }
  }
}

object RunAnalytics extends Runner {
  def tasks(settings: Settings) = new AnalyticsTask(settings) :: Nil
}
