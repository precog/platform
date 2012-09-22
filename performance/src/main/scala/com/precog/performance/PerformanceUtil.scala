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
package com.precog.performance

import annotation.tailrec
import java.io.{PrintStream, PrintWriter}

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import blueeyes.bkka._
import blueeyes.core.service.engines.HttpClientXLightWeb

import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.BijectionsChunkJson._

import akka.dispatch.Await
import akka.util.Duration

import org.joda.time._
import org.joda.time.format._

import scalaz._
import Scalaz._

class PerformanceUtil(apiEndpoint: String, token: String, path: String) {

  val format = ISODateTimeFormat.dateTime 

  class BenchmarkDecomposer[T] extends Decomposer[BenchmarkResults[T]] {
    override def decompose(results: BenchmarkResults[T]): JValue = JObject(List(
      JField("timestamp", format.print(new DateTime(DateTimeZone.UTC))),
      JField("runs", results.testRuns),
      JField("reps", results.repCount),
      JField("mean", results.meanRepTime),
      JField("ptile90", results.ptile(0.9)),
      JField("baseline", results.baseline),
      JField("times", results.timings.serialize)
    ))    
  }

//  class BenchmarkExtractor[T] extends ValidatedExtraction[BenchmarkResults[T]] {    
//    override def validated(obj: JValue): Validation[Error, BenchmarkResults[T]] =
//      ((obj \ "runs").validated[Int] |@|
//       (obj \ "reps").validated[Int] |@|
//       (obj \ "baseline").validated[Double] |@|
//       (obj \ "times").validated[List[Long]].map{ l => Vector(l.toArray:_*) }).apply(BenchmarkResults[T](_,_,_,_))
//  }  
  
  def resultsToJson[T](results: BenchmarkResults[T]): JValue = results.serialize(new BenchmarkDecomposer[T]())

  private val basePath = "vfs/" + path
  private val baseUrl = apiEndpoint + basePath
    

  def uploadResults[T](testId: String, results: BenchmarkResults[T]) {
    val content = resultsToJson(results)
 
    val client = new HttpClientXLightWeb 
    val result = client.path(baseUrl)
      .query("apiKey", token) 
      .contentType(application/MimeTypes.json)
      .post[JValue](testId)(content)

    val r = Await.result(result, Duration(100, "seconds"))
  }

  def main(args: Array[String]) {
    val results = BenchmarkResults[Int](10, 10, 0.0, Vector(100,100,100,100))
    uploadResults[Int]("test_query", results)
    AkkaDefaults.actorSystem.shutdown
  }

}

object PerformanceUtil {
  val matheson = new PerformanceUtil("http://beta2012v1.precog.io/v1/", "D45C1ABE-6B4C-4651-85D1-4FFD783CE1EC", "/matheson/beta/perf/") 
  val precog_test = new PerformanceUtil("http://beta2012v1.precog.io/v1/", "C79AAFEA-C9A5-4451-92CE-EF49E5BB5113", "/perf_results/beta/v1/") 
  val default = precog_test 
}
