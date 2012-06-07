package com.precog.yggdrasil
package actor

import com.precog.common._
import com.precog.util._
import com.precog.yggdrasil.metadata._

import blueeyes.json._
import blueeyes.json.xschema.DefaultSerialization._

import org.specs2.mutable.Specification
import org.specs2.specification.{Fragments, Scope, Step}

import java.io.File

import scala.collection.mutable.MutableList
import scala.collection.immutable.ListMap

import scalaz._
import scalaz.effect._
import Scalaz._

class MetadataStorageSpec extends Specification {
  import FileMetadataStorage._

  val inputMetadata = """{
    "metadata":[],
    "checkpoint":[[1,1]]
  }"""

  val output = inputMetadata
  
  val base = IOUtils.createTmpDir("MetadataStorageSpec").unsafePerformIO

  def cleanupBaseDir = Step {
    IOUtils.recursiveDelete(base)
  }

  override def map(fs: => Fragments) = super.map(fs) ^ cleanupBaseDir

  val colDesc = ColumnDescriptor(Path("/"), JPath(".foo"), CBoolean, Authorities(Set("TOKEN")))

  val desc = ProjectionDescriptor.trustedApply(1, 
               ListMap.empty[ColumnDescriptor, Int] + (colDesc -> 0),
               List((colDesc, ById)))

  val testRecord = MetadataRecord(
    ColumnMetadata.Empty,
    VectorClock.empty.update(1,1)
  )

  trait metadataStore extends Scope {
    val fileOps = new TestFileOps
    val ms = FileMetadataStorage.load(base, fileOps).unsafePerformIO
  }

  "metadata storage" should {
    "safely update metadata" in new metadataStore {
      // First write to add initial data
      ms.findDescriptorRoot(desc, true)
      ms.updateMetadata(desc, testRecord).unsafePerformIO
      
      // Second write to force an update
      val io = for {
        _    <- ms.updateMetadata(desc, testRecord)
        root <- ms.findDescriptorRoot(desc, true) 
      } yield {
        root map { descBase =>
          // Our first write would have added offsets 0-1
          fileOps.confirmWrite(2, new File(descBase, nextFilename), output) aka "write next" must beTrue
          fileOps.confirmCopy(3, new File(descBase, curFilename), new File(descBase, prevFilename)) aka "copy cur to prev" must beTrue 
          fileOps.confirmRename(4, new File(descBase, nextFilename), new File(descBase, curFilename)) aka "move next to cur" must beTrue
        } getOrElse {
          failure("Could not locate the descriptor base")
        }
      }

      io.unsafePerformIO
    }

    "safely update metadata not current" in new metadataStore {
      ms.findDescriptorRoot(desc, true)

      val io = for {
        _    <- ms.updateMetadata(desc, testRecord)
        root <- ms.findDescriptorRoot(desc, true) 
      } yield {
        root map { descBase =>
          fileOps.confirmWrite(0, new File(descBase, nextFilename), output) aka "write next" must beTrue
          fileOps.confirmRename(1, new File(descBase, nextFilename), new File(descBase, curFilename)) aka "move next to cur" must beTrue
        } getOrElse {
          failure("Could not locate the descriptor base")
        }
      }

      io.unsafePerformIO
    }

    "correctly read metadata" in new metadataStore {
      val io = for {
        _ <- ms.findDescriptorRoot(desc, true)
        _ <- ms.updateMetadata(desc, testRecord)
        result <- ms.getMetadata(desc)
      } yield {
        JsonParser.parse(result.serialize) must_== JsonParser.parse(inputMetadata)
      }

      io.unsafePerformIO
    }
  }
}

class TestFileOps extends FileOps {
 
  val messages = MutableList[String]()  

  var backingStore: Map[File,String] = Map()

  def exists(src: File): Boolean = backingStore.keySet.contains(src)

  def rename(src: File, dest: File): IO[Boolean] = {
    messages += "rename %s to %s".format(src, dest)
    backingStore += (dest -> backingStore(src))
    backingStore -= src
    IO(true)
  }
  
  def copy(src: File, dest: File): IO[Unit] = IO {
    messages += "copy %s to %s".format(src, dest)
    backingStore += (dest -> backingStore(src))
  }

  def read(src: File): IO[String] = IO {
    messages += "read from %s".format(src)
    backingStore(src)
  }
  
  def write(dest: File, content: String): IO[Unit] = IO {
    messages += "write to %s with %s".format(dest, content) 
    backingStore += (dest -> content)
  }

  def mkdir(dir: File) = IO(true)

  def checkMessage(i: Int, exp: String) =
    messages.get(i).map { act =>
      val result = act == exp
      if(!result) {
        println("Expected[" + exp.replace(" ", ".") + "] vs Actual[" + act.replace(" ", ".") + "]")
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
