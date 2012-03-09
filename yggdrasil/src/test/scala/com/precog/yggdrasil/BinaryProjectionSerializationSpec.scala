package com.precog.yggdrasil

import java.io.File
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import org.scalacheck._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._

import java.io._
import com.precog.common.VectorCase

import Generators._

object BinaryProjectionSerializationSpec extends Specification with ScalaCheck {
  override val defaultPrettyParams = Pretty.Params(2)

  "serializing an arbitrary projection" should {
    "deserialize to the same projection" in {
      val fs = new BinaryProjectionSerialization {
        def chunkSize = 100
      }

      implicit val arbChunk: Arbitrary[Vector[SEvent]] = Arbitrary(chunk(100, 3, 2))
      implicit val arbStream = listOfN(3, arbitrary[Vector[SEvent]]) map { l => l.toStream } 

      check { (s: Stream[Vector[SEvent]]) =>
        //println(Generators.svcount)
        val expected = s.toList

        val file = File.createTempFile("test", "ygg")
        val written = (fs.writer[Unit, IO](file) &= enumStream[Unit, Vector[SEvent], IO](s)).run(x => sys.error(x.toString)).unsafePerformIO
        val read = (consume[Unit, Vector[SEvent], IO, List] &= fs.reader[Unit](written).apply[IO]).run(x => sys.error(x.toString)).unsafePerformIO

        read must_== expected
      }
    }.set(minTestsOk -> 100)
  }
  
  "serializing an arbitrary projection without a generator" should {
    "deserialize to the same projection" in {
      val fs = new BinaryProjectionSerialization {
        def chunkSize = 100
      }

      val s = List(Vector((VectorCase(55L),SEmptyObject))).toStream

      val expected = s.toList

      val file = File.createTempFile("test", "ygg")
      val written = (fs.writer[Unit, IO](file) &= enumStream[Unit, Vector[SEvent], IO](s)).run(x => sys.error(x.toString)).unsafePerformIO
      val read = (consume[Unit, Vector[SEvent], IO, List] &= fs.reader[Unit](written).apply[IO]).run(x => sys.error(x.toString)).unsafePerformIO 

      read must_== expected
    }
  }

   
  "writeElement" should {
    "read an SObject" in {
      val out = new DataOutputStream(new FileOutputStream("test"))
      val in = new DataInputStream(new FileInputStream("test"))

      val fs = new BinaryProjectionSerialization {
        def chunkSize = 100
      }
      val ev = Vector((VectorCase(18L),SObject(Map("la" -> SString("true")))))

      val write = fs.writeElement(out, ev).unsafePerformIO
      val read = fs.readElement(in).unsafePerformIO getOrElse None

      read must_== ev 

    }
  }    

  "writeElement" should {
    "read an SArray" in {
      val out = new DataOutputStream(new FileOutputStream("test"))
      val in = new DataInputStream(new FileInputStream("test"))

      val fs = new BinaryProjectionSerialization {
        def chunkSize = 100
      }
      val ev = Vector((VectorCase(42L), SArray(Vector(SBoolean(true)))))

      val write = fs.writeElement(out, ev).unsafePerformIO
      val read = fs.readElement(in).unsafePerformIO getOrElse None

      read must_== ev

    }
  }   
  
  "writeElement" should {
    "read an SEmptyArray" in {
      val out = new DataOutputStream(new FileOutputStream("test"))
      val in = new DataInputStream(new FileInputStream("test"))

      val fs = new BinaryProjectionSerialization {
        def chunkSize = 100
      }
      val ev = Vector((VectorCase(42L), SEmptyArray))

      val write = fs.writeElement(out, ev).unsafePerformIO
      val read = fs.readElement(in).unsafePerformIO getOrElse None

      read must_== ev

    }
  } 
}

// vim: set ts=4 sw=4 et:
