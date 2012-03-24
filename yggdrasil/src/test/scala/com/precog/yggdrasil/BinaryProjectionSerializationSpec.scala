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
package com.precog.yggdrasil

import java.io.File
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.specs2.specification.{After,Scope}

import org.scalacheck._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._

import com.weiglewilczek.slf4s.Logging

import java.io._
import com.precog.common.VectorCase
import com.precog.common.util.IOUtils

object BinaryProjectionSerializationSpec extends Specification with ScalaCheck with ArbitrarySValue with Logging {
  override val defaultPrettyParams = Pretty.Params(2)

  def genChunks(size: Int) = LimitList.genLimitList[Vector[SEvent]](size) 

  trait TempFileScope extends Scope with After {
    val file = File.createTempFile("test", "ygg")
    logger.trace("Created BinaryProjectionSerialization work file: " + file)
    
    val fs = new BinaryProjectionSerialization with IterateeFileSerialization[Vector[SEvent]] {
      def chunkSize = 100
    }

    def after = file.delete
  }

  "serializing an arbitrary projection" should {
    implicit val arbChunk: Arbitrary[Vector[SEvent]] = Arbitrary(chunk(100, 3, 2))
    implicit val arbStream = Arbitrary(listOfN(3, arbitrary[Vector[SEvent]]) map { l => l.toStream } )

    "deserialize to the same projection" in {
      check { (s: Stream[Vector[SEvent]]) =>
        val expected = s.toList

        val scope = new TempFileScope() {
          val written = (fs.writer[Unit, IO](file) &= enumStream[Unit, Vector[SEvent], IO](s)).run(x => sys.error(x.toString)).unsafePerformIO
          val read = (consume[Unit, Vector[SEvent], IO, List] &= fs.reader[Unit](written).apply[IO]).run(x => sys.error(x.toString)).unsafePerformIO
        }

        scope.after

        scope.read == expected
      }
    }
  }
  
  "serializing an arbitrary projection without a generator" should {
    "deserialize to the same projection" in new TempFileScope {
      val s = List(Vector((VectorCase(55L),SEmptyObject))).toStream

      val expected = s.toList

      val written = (fs.writer[Unit, IO](file) &= enumStream[Unit, Vector[SEvent], IO](s)).run(x => sys.error(x.toString)).unsafePerformIO
      val read = (consume[Unit, Vector[SEvent], IO, List] &= fs.reader[Unit](written).apply[IO]).run(x => sys.error(x.toString)).unsafePerformIO 
        
      read must_== expected
    }
  }

   
  "writeElement" should {
    "read an SObject" in new TempFileScope {
      val out = new DataOutputStream(new FileOutputStream(file))
      val in = new DataInputStream(new FileInputStream(file))

      val ev = Vector((VectorCase(18L),SObject(Map("la" -> SString("true")))))

      val write = fs.writeElement(out, ev).unsafePerformIO
      val read = fs.readElement(in).unsafePerformIO getOrElse None

      read must_== ev 
    }
  }    

  "writeElement" should {
    "read an SArray" in new TempFileScope {
      val out = new DataOutputStream(new FileOutputStream(file))
      val in = new DataInputStream(new FileInputStream(file))

      val ev = Vector((VectorCase(42L), SArray(Vector(SBoolean(true)))))

      val write = fs.writeElement(out, ev).unsafePerformIO
      val read = fs.readElement(in).unsafePerformIO getOrElse None
      
      read must_== ev
    }
  }   
  
  "writeElement" should {
    "read an SEmptyArray" in new TempFileScope {
      val out = new DataOutputStream(new FileOutputStream(file))
      val in = new DataInputStream(new FileInputStream(file))

      val ev = Vector((VectorCase(42L), SEmptyArray))

      val write = fs.writeElement(out, ev).unsafePerformIO
      val read = fs.readElement(in).unsafePerformIO getOrElse None
      
      read must_== ev
    }
  } 
}

// vim: set ts=4 sw=4 et:
