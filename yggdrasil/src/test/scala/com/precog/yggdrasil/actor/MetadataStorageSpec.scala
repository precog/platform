package com.precog.yggdrasil
package actor

import com.precog.common._
import com.precog.util._
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

  import MetadataStorage._

  val inputMetadata = 
"""{
  "metadata":[],
  "checkpoint":[[1,1]]
}"""
  val output = inputMetadata
  
  val base = new File("/test/col")

  val colDesc = ColumnDescriptor(Path("/"), JPath(".foo"), CBoolean, Authorities(Set("TOKEN")))

  val desc = ProjectionDescriptor.trustedApply(1, 
               ListMap.empty[ColumnDescriptor, Int] + (colDesc -> 0),
               List((colDesc, ById)))

  val testRecord = MetadataRecord(
    ColumnMetadata.Empty,
    VectorClock.empty.update(1,1)
  )

  "metadata storage" should {
    "safely update metadata" in {
      val ms = new TestFileMetadataStorage(inputMetadata, base, List(new File(base, curFilename)))
      val result = ms.updateMetadata(desc, testRecord).unsafePerformIO
      result must beLike {
        case Success(()) => ok
      }
      ms.fileOps.confirmWrite(0, new File(base, nextFilename), output) aka "write next" must beTrue
      ms.fileOps.confirmCopy(1, new File(base, curFilename), new File(base, prevFilename)) aka "copy cur to prev" must beTrue 
      ms.fileOps.confirmRename(2, new File(base, nextFilename), new File(base, curFilename)) aka "move next to cur" must beTrue
    }
    "safely update metadata no current" in {
      val ms = new TestFileMetadataStorage(inputMetadata, base, List())
      val result = ms.updateMetadata(desc, testRecord).unsafePerformIO
      result must beLike {
        case Success(()) => ok
      }
      ms.fileOps.confirmWrite(0, new File(base, nextFilename), output) aka "write next" must beTrue
      ms.fileOps.confirmRename(1, new File(base, nextFilename), new File(base, curFilename)) aka "move next to cur" must beTrue
    }
    "correctly read metadata" in {
      val ms = new TestFileMetadataStorage(inputMetadata, base, List(new File(base, curFilename)))
      val result = ms.currentMetadata(desc).unsafePerformIO
      result must beLike {
       case Success(m) => Printer.pretty(Printer.render(m.serialize)) must_== inputMetadata
      }
    }
  }
}

class TestFileMetadataStorage(val input: String, dirName: File, val existing: List[File]) extends FileMetadataStorage { self =>
  object fileOps extends TestFileOps {
    def input = self.input
    def existing = self.existing
  }

  val dirMapping = (_: ProjectionDescriptor) => IO { dirName }
}

trait TestFileOps extends FileOps {
 
  def input: String
  def existing: List[File]
 
  val messages = MutableList[String]()  

  def exists(src: File): Boolean = existing.contains(src)

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
