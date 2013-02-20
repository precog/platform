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

import kafka._
import java.nio.ByteBuffer

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen._

import blueeyes.json._
import blueeyes.json.serialization._

import scalaz._

object EventMessageSerializationSpec extends Specification with ScalaCheck with ArbitraryEventMessage {
  implicit val arbMsg = Arbitrary(genRandomEventMessage)
  
  "Event message serialization " should {
    "maintain event content" in { check { (in: EventMessage) => 
      val buf = EventMessageEncoding.toMessageBytes(in)
      EventMessageEncoding.read(buf) must beLike {
        case Success(\/-(out)) => out must_== in
        case Failure(Extractor.Thrown(ex)) => throw ex
      }
    }}
  }
}
