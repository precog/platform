package com.precog.yggdrasil
package actor

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil.metadata._

import blueeyes.json._
import blueeyes.json.xschema.DefaultSerialization._

import org.specs2.mutable.Specification

import java.io.File

import scala.collection.mutable.MutableList
import scala.collection.immutable.ListMap

import scalaz._
import scalaz.effect._
import Scalaz._

class MetadataStorageSpec extends Specification {

  val inputMetadata = 
"""{
  "metadata":[],
  "checkpoint":[[1,1]]
}"""
  val output = inputMetadata
  
  val base = new File("/test/col")

  val colDesc = ColumnDescriptor(Path("/"), JPath(".foo"), SBoolean, Authorities(Set("TOKEN")))

  val desc = ProjectionDescriptor.trustedApply(1, 
               ListMap.empty[ColumnDescriptor, Int] + (colDesc -> 0),
               List((colDesc, ById)))

  val testRecord = MetadataRecord(
    ColumnMetadata.Empty,
    VectorClock.empty.update(1,1)
  )

  "metadata storage" should {
    "safely update metadata" in {
      val ms = new TestMetadataStorage(inputMetadata, base)
      val result = ms.updateMetadata(desc, testRecord).unsafePerformIO
      result must beLike {
        case Success(()) => ok
      }
      ms.confirmWrite(0, new File(base, ms.nextFilename), output) aka "write next" must beTrue
      ms.confirmCopy(1, new File(base, ms.curFilename), new File(base, ms.prevFilename)) aka "copy cur to prev" must beTrue 
      ms.confirmRename(2, new File(base, ms.nextFilename), new File(base, ms.curFilename)) aka "move next to cur" must beTrue
    }
    "correctly read metadata" in {
      val ms = new TestMetadataStorage(inputMetadata, base)
      val result = ms.currentMetadata(desc).unsafePerformIO
      result must beLike {
       case Success(m) => Printer.pretty(Printer.render(m.serialize)) must_== inputMetadata
      }
    }
  }
}

class TestMetadataStorage(val input: String, dirName: File) extends MetadataStorage with TestFileOps {
  val dirMapping = (_: ProjectionDescriptor) => IO { dirName }
}

trait TestFileOps {
 
  def input: String
 
  val messages = MutableList[String]()  

  def rename(src: File, dest: File): Unit = {
    messages += "rename %s to %s".format(src, dest)
  }
  
  def copy(src: File, dest: File): IO[Validation[Throwable, Unit]] = IO {
    messages += "copy %s to %s".format(src, dest)
    Success(())
  }

  def read(src: File): IO[Option[String]] = IO {
    messages += "read from %s".format(src)
    Some(input) 
  }
  
  def write(dest: File, content: String): IO[Validation[Throwable, Unit]] = IO {
    messages += "write to %s with %s".format(dest, content) 
    Success(()) 
  }

  def checkMessage(i: Int, exp: String) =
    messages.get(i).map { act =>
      val result = act == exp
      if(!result) {
        println("Expected [" + exp.replace(" ", ".") + "] vs Actual[" + act.replace(" ", ".") + "]")
      }
      act == exp 
    } getOrElse { false }

  def confirmRename(i: Int, src: File, dest: File) = 
    checkMessage(i, "rename %s to %s".format(src, dest))

  def confirmCopy(i: Int, src: File, dest: File) =
    checkMessage(i, "copy %s to %s".format(src, dest))
  
  def confirmRead(i: Int, src: File) =
    checkMessage(i, "read from %s".format(src))

  def confirmWrite(i: Int, dest: File, content: String) =
    checkMessage(i, "write to %s with %s".format(dest, content))
  
}
