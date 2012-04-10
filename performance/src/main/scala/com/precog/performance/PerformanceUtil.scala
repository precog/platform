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
      .query("tokenId", token) 
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
  val precog_test = new PerformanceUtil("http://beta2012v1.precog.io/v1/", "C79AAFEA-C9A5-4451-92CE-EF49E5BB5113", "/perf_results/beta/v1/") 
  val default = precog_test 
}
