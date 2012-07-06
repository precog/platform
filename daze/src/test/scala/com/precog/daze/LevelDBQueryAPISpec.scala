package com.precog
package daze

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import java.io.File
import scalaz.effect._
import scalaz.std.anyVal._
import scalaz.std.list._

import org.specs2.mutable._

trait StubLevelDBQueryComponent extends LevelDBQueryComponent 
with StubYggShardComponent with IterableDatasetOpsComponent {
  trait YggConfig extends SortConfig with LevelDBQueryConfig with IterableDatasetOpsConfig {
    val projectionRetrievalTimeout = Timeout(intToDurationInt(10).seconds)
    val clock = blueeyes.util.Clock.System
    val sortBufferSize: Int = 1000
    val sortWorkDir: File = new File("/tmp")
  }

  override type Dataset[E] = IterableDataset[E]

  implicit val actorSystem: ActorSystem = ActorSystem("leveldbQueryApiSpec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  def sampleSize = 1 

  val testUID = "testUID"
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]) = IterableDataset(idCount, data)

  object yggConfig extends YggConfig

  object query extends QueryAPI
  object ops extends Ops
}

class LevelDBQueryAPISpec extends Specification with StubLevelDBQueryComponent {
  object storage extends Storage

  "fullProjection" should {
    "return all of the objects inserted into projections" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000, new Release(IO(())))
      dataset.iterator.toSeq must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "mask" should {
    "descend" in {
      val dataset = query.mask(testUID, dataPath).derefObject("gender").realize(System.currentTimeMillis + 10000, new Release(IO(())))
      dataset.iterator.toSeq must haveTheSameElementsAs(storage.sampleData.map(v => fromJValue(v \ "gender")))
    }
  }
}

class LevelDBNullMergeSpec extends Specification with StubLevelDBQueryComponent {
  object storage extends Storage {
    override lazy val sampleData: Vector[JValue] = Vector(
      JsonParser.parse("""[
        {"foo": {
          "bar": { "baz": 1 }
        }},
        {"foo": null}
      ]""" ).asInstanceOf[JArray].elements: _*)
  }

  "fullProjection" should {
    "restore objects with null components" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000, new Release(IO(())))
      
      dataset.iterator.toSeq must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }
}

class LevelDBNestedMergeSpec extends Specification with StubLevelDBQueryComponent {
  object storage extends Storage {
    override lazy val sampleData: Vector[JValue] = Vector(
      JsonParser.parse( 
        """[{
         "event":"activated",
         "currency":"USD",
         "customer":{
           "country":"CA",
           "email":"john@fastspring.com",
           "firstName":"John",
           "lastName":"Smith",
           "organization":"",
           "zipcode":"11111"
         },
         "endDate":null,
         "product":{
           "name":"Subscription 1"
         },
         "quantity":1,
         "regularPriceUsd":10,
         "timestamp":{
           "date":7,
           "day":3,
           "hours":0,
           "minutes":0,
           "month":2,
           "seconds":0,
           "time":1331078400000,
           "timezoneOffset":0,
           "year":112
         }
        },{
         "event":"deactivated",
         "currency":"USD",
         "customer":{
           "country":"US",
           "email":"ryan@fastspring.com",
           "firstName":"Ryan",
           "lastName":"Dewell",
           "organization":"",
           "zipcode":"93101"
         },
         "endDate":{
           "date":7,
           "day":3,
           "hours":0,
           "minutes":0,
           "month":2,
           "seconds":0,
           "time":1331078400000,
           "timezoneOffset":0,
           "year":112
         },
         "product":{
           "name":"ABC Subscription"
         },
         "quantity":1,
         "reason":"canceled",
         "regularPriceUsd":9,
         "timestamp":{
           "date":7,
           "day":3,
           "hours":0,
           "minutes":0,
           "month":2,
           "seconds":0,
           "time":1331078400000,
           "timezoneOffset":0,
           "year":112
         }
        }]"""
      ).asInstanceOf[JArray].elements: _*
    )
  }

  "fullProjection" should {
    "restore objects with null components" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000, new Release(IO(())))
      
      dataset.iterator.toSeq must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }
}
