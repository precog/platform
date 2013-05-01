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

  def genStoreMode: Gen[StoreMode] = 
      Gen.oneOf(StoreMode.Create, StoreMode.Replace, StoreMode.Append)

  def genStreamRef: Gen[StreamRef] = 
    for {
      terminal <- arbitrary[Boolean]
      storeMode <- genStoreMode
    } yield storeMode.createStreamRef(terminal)

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
