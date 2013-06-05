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

  val simpleData = """
    {"a":1,"b":"Tom"}
    {"a":2,"b":3}
    {"a":3,"b":true}
    {"a":4,"b":null}
    {"a":5,"c":"asdf"}
  """

  "metadata web service" should {

    // A user just set up an account and ingested some data, and wants to see
    // what the metadata looks like. It should not be surprising.
    "retrieve full metadata of simple path" in {

      // 1. Create an account.
      // 2. Ingest some data.
      // 3. Wait for data to be ingested.
      // 4. Ensure metadata returns what we expect.

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

    // The user wants to see how many "numbers" are at the property .a. They
    // should be able to drill down and get this number using the metadata API.
    "count a numeric property correctly" in {

      // 1. Create an account.
      // 2. Ingest data to root path.
      // 3. Query structure for the property .a
      // 4. We should get { "structure": { "types": { "Number": 5 } } } back.

      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey, Some("structure"), Some("a"))(_ / account.bareRootPath / "")
        val types = json \ "structure" \ "types"
        (types \ "Number").deserialize[Long] must_== 5L
      }
    }

    // User attempts to determine the structure of paths they have access to.
    // They may query the root path, and several sub-paths. This kind of
    // exploration should be possible. They also should see anyone elses paths
    // while doing this, even if they exist in the same parent path.
    "return children for subpaths" in {

      // 1. Create an account.
      // 2. Ingest data to several paths:
      //     - /'accountId/foo
      //     - /'accountId/foo/bar
      //     - /'accountId/bar
      // 3. Ensure we can see foo/ and bar/ from /'accountId/
      // 4. Ensure we can see bar/ from /'accountId/foo/
      // 5. Ensure we can see 'accountId/ from /

      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "")
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "bar" / "")
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "bar" / "")

      val account2 = createAccount
      ingestString(account2, simpleData, "application/json")(_ / account2.bareRootPath / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / "")
        val subpaths = (json \ "children").children map (_.deserialize[String])
        subpaths must haveTheSameElementsAs(List(account.bareRootPath + "/"))

        val json2 = metadataFor(account2.apiKey)(_ / "")
        val subpaths2 = (json2 \ "children").children map (_.deserialize[String])
        subpaths2 must haveTheSameElementsAs(List(account2.bareRootPath + "/"))
      }

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

    // User tries to retrieve metadata of a path they don't have permissions to
    // access. They should get a valid response from the server, but it should
    // look as if the path is empty.
    "forbid retrieval of metadata from unrelated API key" in {

      // 1. Create account Adam.
      // 2. Create account Eve.
      // 3. Ingest some data as Adam.
      // 4. Wait for data to be fully ingested (Adam should see 5 records).
      // 5. Query metadata as Eve.
      // 6. No useful data should be returned!

      val adam = createAccount
      val eve = createAccount
      ingestString(adam, simpleData, "application/json")(_ / adam.bareRootPath / "")

      EventuallyResults.eventually(10, 1.second) {
        val json1 = metadataFor(adam.apiKey)(_ / adam.bareRootPath / "")
        (json1 \ "size").deserialize[Long] must_== 5

        val json2 = metadataFor(eve.apiKey)(_ / adam.bareRootPath / "")
        //println(json2)
        json2 must_== JUndefined
      }
    }

    // User ingests some data, deletes it, then checks metadata of deleted path.
    // After the data is deleted, the path metadata should look empty again.
    "metadata respects deleted data" in {

      // 1. Create an account.
      // 2. Ingest some data.
      // 3. Wait for data to be ingested fully.
      // 4. Delete the data at the path.
      // 5. Wait and see if the metadata eventually sees the deletion.

      val account = createAccount
      ingestString(account, simpleData, "application/json")(_ / account.bareRootPath / "foo" / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
        (json \ "size").deserialize[Long] must_== 5
      }

      deletePath(account.apiKey)(_ / account.bareRootPath / "foo" / "")

      EventuallyResults.eventually(10, 1.second) {
        val json = metadataFor(account.apiKey)(_ / account.bareRootPath / "foo" / "")
        (json \ "size").deserialize[Long] must_== 0
      }
    }
  }
}

object RunMetadata extends Runner {
  def tasks(settings: Settings) = new MetadataTask(settings) :: Nil
}
