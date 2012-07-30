package com.precog.yggdrasil
package iterable

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import java.io.File
import scalaz.Id._
import scalaz.effect.IO

import org.specs2.mutable._

class LevelDBQueryAPISpec extends Specification with StubLevelDBQueryComponent {
  object storage extends Storage

  "fullProjection" should {
    "return all of the objects inserted into projections" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000, new Release(IO(())))
      dataset.iterator.toSeq must haveTheSameElementsAs(sampleData.map(fromJValue))
    }
  }

  "mask" should {
    "descend" in {
      val dataset = query.mask(testUID, dataPath).derefObject("gender").realize(System.currentTimeMillis + 10000, new Release(IO(())))
      dataset.iterator.toSeq must haveTheSameElementsAs(sampleData.map(v => fromJValue(v \ "gender")))
    }
  }
}

class LevelDBNullMergeSpec extends Specification with StubLevelDBQueryComponent {
  object storage extends Storage

  override val sampleData: Vector[JValue] = Vector(
    JsonParser.parse("""[
      {"foo": {
        "bar": { "baz": 1 }
      }},
      {"foo": null}
    ]""" ).asInstanceOf[JArray].elements: _*)

  "fullProjection" should {
    "restore objects with null components" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000, new Release(IO(())))
      
      dataset.iterator.toSeq must haveTheSameElementsAs(sampleData.map(fromJValue))
    }
  }
}

class LevelDBNestedMergeSpec extends Specification with StubLevelDBQueryComponent {
  object storage extends Storage

  override val sampleData: Vector[JValue] = Vector(
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

  "fullProjection" should {
    "restore objects with null components" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000, new Release(IO(())))
      
      dataset.iterator.toSeq must haveTheSameElementsAs(sampleData.map(fromJValue))
    }
  }
}
