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

  val inputMetadata = 
"""{
  "metadata":[],
  "checkpoint":[[1,1]]
}"""
  val output = inputMetadata
  
  val base = IOUtils.createTmpDir("MetadataStorageSpec")

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
    val ms = new FileMetadataStorage(base, fileOps)
  }

  "metadata storage" should {
    "safely update metadata" in new metadataStore {
      // First write to add initial data
      ms.updateMetadata(desc, testRecord).unsafePerformIO
      
      // Second write to force an update
      val result = ms.updateMetadata(desc, testRecord).unsafePerformIO
      result must beLike {
        case Success(()) => ok
      }

      ms.findDescriptorRoot(desc) map { descBase =>
        // Our first write would have added offsets 0-1
        fileOps.confirmWrite(2, new File(descBase, nextFilename), output) aka "write next" must beTrue
        fileOps.confirmCopy(3, new File(descBase, curFilename), new File(descBase, prevFilename)) aka "copy cur to prev" must beTrue 
        fileOps.confirmRename(4, new File(descBase, nextFilename), new File(descBase, curFilename)) aka "move next to cur" must beTrue
      } getOrElse {
        failure("Could not locate the descriptor base")
      }
    }

    "safely update metadata not current" in new metadataStore {
      val result = ms.updateMetadata(desc, testRecord).unsafePerformIO

      result must beLike {
        case Success(()) => ok
      }

      ms.findDescriptorRoot(desc) map { descBase =>
        fileOps.confirmWrite(0, new File(descBase, nextFilename), output) aka "write next" must beTrue
        fileOps.confirmRename(1, new File(descBase, nextFilename), new File(descBase, curFilename)) aka "move next to cur" must beTrue
      } getOrElse {
        failure("Could not locate the descriptor base")
      }
    }

    "correctly read metadata" in new metadataStore {
      // Add metadata in first
      ms.updateMetadata(desc, testRecord).unsafePerformIO

      val result = ms.currentMetadata(desc).unsafePerformIO

      result must beLike {
       case Success(m) => Printer.pretty(Printer.render(m.serialize)) must_== inputMetadata
      }
    }
  }
}

class TestFileOps extends FileOps {
 
  val messages = MutableList[String]()  

  var backingStore: Map[File,String] = Map()

  def exists(src: File): Boolean = backingStore.keySet.contains(src)

  def rename(src: File, dest: File): Unit = {
    messages += "rename %s to %s".format(src, dest)
    backingStore += (dest -> backingStore(src))
    backingStore -= src
  }
  
  def copy(src: File, dest: File): IO[Validation[Throwable, Unit]] = IO {
    messages += "copy %s to %s".format(src, dest)
    backingStore += (dest -> backingStore(src))
    Success(())
  }

  def read(src: File): IO[Option[String]] = IO {
    messages += "read from %s".format(src)
    Some(backingStore(src)) 
  }
  
  def write(dest: File, content: String): IO[Validation[Throwable, Unit]] = IO {
    messages += "write to %s with %s".format(dest, content) 
    backingStore += (dest -> content)
    Success(()) 
  }

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
