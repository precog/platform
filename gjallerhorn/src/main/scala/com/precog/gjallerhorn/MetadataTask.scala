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

class MetadataTask(settings: Settings) extends Task(settings: Settings) with Specification {

  def metadataFor(apiKey: String, tpe: Option[String] = None, prop: Option[String] = None)(f: Req => Req): JValue = {
    val params = List(
      Some("apiKey" -> apiKey),
      tpe map ("type" -> _),
      prop map ("property" -> _)
    ).flatten
    val req = f(metadata / "fs") <<? params
    val res = Http(req OK as.String)
    val json = JParser.parseFromString(res()).valueOr(throw _)
    json
  }

  val simpleData = """
    {"a":1,"b":"Tom"}
    {"a":2,"b":3}
    {"a":3,"b":true}
    {"a":4,"b":null}
    {"a":5,"c":"asdf"}
  """

  // Specs2 has or combinator.

  "metadata web service" should {
    "retrieve full metadata of simple path" in {
      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
        (json \ "size").deserialize[Long] must_== 5
        (json \ "children").children map (_.deserialize[String]) must_== Nil
        val cPathChildren = (json \ "structure" \ "children").children map (_.deserialize[String])
        cPathChildren must haveTheSameElementsAs(List(".a", ".b", ".c"))
        (json \ "strucutre" \ "types").children must_== Nil
      }
    }

    "count a property correctly" in {
      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey, Some("structure"), Some("a"))(_ / account.bareRootPath / "")
        val types = json \ "structure" \ "types"
        (types \ "Number").deserialize[Long] must_== 5L
      }
    }

    "return children for subpaths" in {
      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "")
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "bar" / "")
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "bar" / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "")
        val subpaths = (json \ "children").children map (_.deserialize[String])
        subpaths must haveTheSameElementsAs(List("foo/", "bar/"))
      }

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
        val subpaths = (json \ "children").children map (_.deserialize[String])
        subpaths must haveTheSameElementsAs(List("bar/"))
      }
    }

    "forbid retrieval of metadata from unrelated API key" in {
      val adam = createAccount
      val eve = createAccount
      ingestString(adam, simpleData, "application/json")(_ / adam.bareRootPath / "")

      EventuallyResults.eventually(10, 1.second) {
        val json1 = metadataFor(adam.apiKey)(_ / adam.bareRootPath / "")
        (json1 \ "size").deserialize[Long] must_== 5

        val json2 = metadataFor(eve.apiKey)(_ / adam.bareRootPath / "")
        println(json2)
        (json2 \ "size").deserialize[Long] must_== 0
      }
    }
  }
}
