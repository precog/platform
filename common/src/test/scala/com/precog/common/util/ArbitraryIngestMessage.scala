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
package util

import security._

import java.util.concurrent.atomic.AtomicInteger

import blueeyes.json._

import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

trait ArbitraryIngestMessage extends ArbitraryJValue {
  def genContentJValue: Gen[JValue] = frequency((1, genSimple), (1, wrap(choose(0, 5) flatMap genArray)), (1, wrap(choose(0, 5) flatMap genObject)))
  
  def genPath: Gen[List[String]] = Gen.resize(10, Gen.containerOf[List, String](alphaStr))

  def genRandomEvent: Gen[Event] = for(path <- genPath; apiKey <- alphaStr; content <- genContentJValue) yield Event.fromJValue(Path("/" + path.filter(_.length != 0).mkString("/")), content, apiKey)
  
  def genRandomEventMessage: Gen[EventMessage] = for(producerId <- choose(0,1000000); eventId <- choose(0, 1000000); event <- genRandomEvent) 
                                           yield EventMessage(producerId, eventId, event)
  
//  def genRandomSyncMessage: Gen[SyncMessage] = for(producerId <- choose(0, 1000000); syncId <- choose(0, 10000); ids <- Gen.resize(100, Gen.containerOf[List, Int](choose(0,1000000))))
//                                         yield SyncMessage(producerId, syncId, ids)
  
  //def genRandomIngestMessage: Gen[IngestMessage] = frequency((1, genRandomSyncMessage), (10, genRandomEventMessage))
  def genRandomIngestMessage: Gen[IngestMessage] = genRandomEventMessage

}

trait RealisticIngestMessage extends ArbitraryIngestMessage {
  val rootAPIKey: APIKey 
  
  def buildBoundedPaths(depth: Int): List[String] = {
    buildChildPaths(List.empty, depth).map("/" + _.reverse.mkString("/"))
  }
  
  def buildBoundedJPaths(depth: Int): List[JPath] = {
    buildChildPaths(List.empty, depth).map(_.reverse.mkString(".")).filter(_.length > 0).map(JPath(_))
  }
  
  def buildChildPaths(parent: List[String], depth: Int): List[List[String]] = {
    if(depth == 0) List(parent)
    else
      parent ::
      containerOfN[List, String](choose(2,4).sample.get, resize(10, alphaStr)).map(_.filter(_.length > 1).flatMap(child => buildChildPaths(child :: parent, depth - 1))).sample.get
  }

  val producers = 4
  
  val eventIds = Map[Int, AtomicInteger](
      0.until(producers).map((_, new AtomicInteger)).toArray[(Int, AtomicInteger)]: _*
  )
  
  def genEventMessage: Gen[EventMessage] = for(producerId <- choose(0,producers-1); event <- genEvent) yield EventMessage(producerId, eventIds(producerId).getAndIncrement, event) 
  
  def genEvent: Gen[Event] = for (path <- genStablePath; event <- genRawEvent) yield Event.fromJValue(Path(path), event, rootAPIKey)
  
  def genRawEvent: Gen[JValue] = containerOfN[Set, JPath](10, genStableJPath).map(_.map((_, genSimpleNotNull.sample.get)).foldLeft[JValue](JObject(Nil)){ (acc, t) =>
      acc.set(t._1, t._2)
    })
    
  val paths = buildBoundedPaths(3)
  val jpaths = buildBoundedJPaths(3)
  
  def genStablePaths: Gen[Seq[String]] = lzy(paths)
  def genStableJPaths: Gen[Seq[JPath]] = lzy(jpaths)
  
  def genStablePath: Gen[String] = oneOf(paths)
  def genStableJPath: Gen[JPath] = oneOf(jpaths)

  // - sync messages that actually reflect the stream of events
  // - consider the introduction of periodic errors
  // -- malformed message
  // -- missing sync
  // -- missing message
  // -- out of order sync
  // -- out of order message
  
}
