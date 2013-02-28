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

import blueeyes.json._

import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

trait ArbitraryEventMessage extends ArbitraryJValue {
  def genContentJValue: Gen[JValue] =
    frequency(
      (1, genSimple),
      (1, wrap(choose(0, 5) flatMap genArray)),
      (1, wrap(choose(0, 5) flatMap genObject))
    )

  def genPath: Gen[Path] = Gen.resize(10, Gen.containerOf[List, String](alphaStr)) map { elements =>
    Path(elements.filter(_.length > 0))
  }

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
    } yield Ingest(apiKey, path, Some(ownerAccountId), content, jobId)

  def genRandomArchive: Gen[Archive] =
    for {
      apiKey <- alphaStr
      path <- genPath
      jobId <- oneOf(identifier.map(Option.apply), None)
    } yield Archive(apiKey, path, jobId)

  def genRandomIngestMessage: Gen[IngestMessage] =
    for {
      ingest <- genRandomIngest
      eventIds <- containerOfN[List, EventId](ingest.data.size, genEventId).map(l => Vector(l: _*))
    } yield {
      //TODO: Replace with IngestMessage.fromIngest when it's usable
      val data = (eventIds zip ingest.data) map { Function.tupled(IngestRecord.apply) }
      IngestMessage(ingest.apiKey, ingest.path, Authorities(ingest.ownerAccountId.get), data, ingest.jobId)
    }

  def genRandomArchiveMessage: Gen[ArchiveMessage] =
    for {
      eventId <- genEventId
      archive <- genRandomArchive
    } yield ArchiveMessage(eventId, archive)

  def genRandomEventMessage: Gen[EventMessage] =
    frequency(
      (1, genRandomArchiveMessage),
      (10, genRandomIngestMessage)
    )
}

trait RealisticEventMessage extends ArbitraryEventMessage {
  val ingestAPIKey: APIKey
  val ingestOwnerAccountId: Option[AccountId]

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
  } yield Ingest(ingestAPIKey, Path(path), ingestOwnerAccountId, ingestData, None)

  def genIngestMessage: Gen[IngestMessage] = for {
    producerId <- choose(0, producers-1)
    ingest <- genIngest
  } yield {
    val records = ingest.data map { jv => IngestRecord(EventId(producerId, eventIds(producerId).getAndIncrement), jv) }
    IngestMessage(ingest.apiKey, ingest.path, Authorities(ingest.ownerAccountId.get), records, ingest.jobId)
  }
}
