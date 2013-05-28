package com.precog.common
package ingest

import accounts.AccountId
import security._
import util.ArbitraryJValue

import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID

import blueeyes.json._

import org.joda.time.Instant
import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

trait ArbitraryEventMessage extends ArbitraryJValue {
  def genStreamId: Gen[Option[UUID]] = Gen.oneOf(Gen.resultOf[Int, Option[UUID]](_ => Some(UUID.randomUUID)), None)

  def genContentJValue: Gen[JValue] =
    frequency(
      (1, genSimple),
      (1, wrap(choose(0, 5) flatMap genArray)),
      (1, wrap(choose(0, 5) flatMap genObject))
    )

  def genPath: Gen[Path] = Gen.resize(10, Gen.containerOf[List, String](alphaStr)) map { elements =>
    Path(elements.filter(_.length > 0))
  }

  def genWriteMode: Gen[WriteMode] = 
      Gen.oneOf(AccessMode.Create, AccessMode.Replace, AccessMode.Append)

  def genStreamRef: Gen[StreamRef] = 
    for {
      terminal <- arbitrary[Boolean]
      storeMode <- genWriteMode
    } yield StreamRef.forWriteMode(storeMode, terminal)

  def genEventId: Gen[EventId] =
    for {
      producerId <- choose(0,1000000)
      sequenceId <- choose(0, 1000000)
    } yield EventId(producerId, sequenceId)

  def genRandomIngest: Gen[Ingest] =
    for {
      apiKey <- alphaStr
      path <- genPath
      ownerAccountId <- alphaStr
      content <- containerOf[List, JValue](genContentJValue).map(l => Vector(l: _*)) if !content.isEmpty
      jobId <- oneOf(identifier.map(Option.apply), None)
      streamRef <- genStreamRef
    } yield Ingest(apiKey, path, Some(Authorities(ownerAccountId)), content, jobId, new Instant(), streamRef)

  def genRandomArchive: Gen[Archive] =
    for {
      apiKey <- alphaStr
      path <- genPath
      jobId <- oneOf(identifier.map(Option.apply), None)
    } yield Archive(apiKey, path, jobId, new Instant())

  def genRandomIngestMessage: Gen[IngestMessage] =
    for {
      ingest <- genRandomIngest if ingest.writeAs.isDefined
      eventIds <- containerOfN[List, EventId](ingest.data.size, genEventId).map(l => Vector(l: _*))
      streamRef <- genStreamRef
    } yield {
      //TODO: Replace with IngestMessage.fromIngest when it's usable
      val data = (eventIds zip ingest.data) map { Function.tupled(IngestRecord.apply) }
      IngestMessage(ingest.apiKey, ingest.path, ingest.writeAs.get, data, ingest.jobId, new Instant(), streamRef)
    }

  def genRandomArchiveMessage: Gen[ArchiveMessage] =
    for {
      eventId <- genEventId
      archive <- genRandomArchive
    } yield ArchiveMessage(archive.apiKey, archive.path, archive.jobId, eventId, archive.timestamp)

  def genRandomEventMessage: Gen[EventMessage] =
    frequency(
      (1, genRandomArchiveMessage),
      (10, genRandomIngestMessage)
    )
}

trait RealisticEventMessage extends ArbitraryEventMessage {
  val ingestAPIKey: APIKey
  val ingestOwnerAccountId: Authorities

  lazy val producers = 4

  lazy val eventIds: Map[Int, AtomicInteger] = 0.until(producers).map(_ -> new AtomicInteger).toMap

  lazy val paths = buildBoundedPaths(3)
  lazy val jpaths = buildBoundedJPaths(3)

  def buildBoundedPaths(depth: Int): List[String] = {
    buildChildPaths(List.empty, depth).map("/" + _.reverse.mkString("/"))
  }

  def buildBoundedJPaths(depth: Int): List[JPath] = {
    buildChildPaths(List.empty, depth).map(_.reverse.mkString(".")).filter(_.length > 0).map(JPath(_))
  }

  def buildChildPaths(parent: List[String], depth: Int): List[List[String]] = {
    if (depth == 0) {
      List(parent)
    } else {
      parent ::
      containerOfN[List, String](choose(2,4).sample.get, resize(10, alphaStr)).map(_.filter(_.length > 1).flatMap(child => buildChildPaths(child :: parent, depth - 1))).sample.get
    }
  }

  def genStablePaths: Gen[Seq[String]] = lzy(paths)
  def genStableJPaths: Gen[Seq[JPath]] = lzy(jpaths)

  def genStablePath: Gen[String] = oneOf(paths)
  def genStableJPath: Gen[JPath] = oneOf(jpaths)

  def genIngestData: Gen[JValue] = for {
    paths  <- containerOfN[Set, JPath](10, genStableJPath)
    values <- containerOfN[Set, JValue](10, genSimpleNotNull)
  } yield {
    (paths zip values).foldLeft[JValue](JObject(Nil)) {
      case (obj, (path, value)) => obj.set(path, value)
    }
  }

  def genIngest: Gen[Ingest] = for {
    path <- genStablePath
    ingestData <- containerOf[List, JValue](genIngestData).map(l => Vector(l: _*))
    streamRef <- genStreamRef
  } yield Ingest(ingestAPIKey, Path(path), Some(ingestOwnerAccountId), ingestData, None, new Instant(), streamRef)

  def genIngestMessage: Gen[IngestMessage] = for {
    producerId <- choose(0, producers-1)
    ingest <- genIngest
  } yield {
    val records = ingest.data map { jv => IngestRecord(EventId(producerId, eventIds(producerId).getAndIncrement), jv) }
    IngestMessage(ingest.apiKey, ingest.path, ingest.writeAs.get, records, ingest.jobId, ingest.timestamp, ingest.streamRef)
  }
}
