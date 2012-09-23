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

import metadata._
import com.precog.common._
import com.precog.util._

import blueeyes.json.JPath
import blueeyes.concurrent.test._
import blueeyes.json.xschema.Extractor._

import java.io.File

import akka.pattern.ask
import akka.actor.{Actor, ActorSystem, Props}
import akka.dispatch._
import akka.util._

import com.weiglewilczek.slf4s.Logging

import scala.collection.immutable.ListMap

import scalaz.Success
import scalaz.effect._
import scalaz.std.map._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._

import org.specs2.mock.Mockito
import org.specs2.mutable._
import akka.testkit.TestActorRef

class TestMetadataStorage(data: Map[ProjectionDescriptor, ColumnMetadata]) extends MetadataStorage {
  var updates: Map[ProjectionDescriptor, Seq[MetadataRecord]] = Map()

  def findDescriptorRoot(desc: ProjectionDescriptor, createOk: Boolean): IO[Option[File]] = IO(None)
  def findArchiveRoot(desc: ProjectionDescriptor): IO[Option[File]] = IO(None)
  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor] = data.keySet.filter(f)

  def getMetadata(desc: ProjectionDescriptor): IO[MetadataRecord] = IO {
    updates.get(desc).map(_.last).orElse(data.get(desc).map(MetadataRecord(_, VectorClock.empty))).get
  }

  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[Unit] = IO {
    updates += (desc -> (updates.getOrElse(desc, Vector.empty[MetadataRecord]) :+ metadata))
  }

  def archiveMetadata(desc: ProjectionDescriptor): IO[Unit] = IO {
    updates -= desc
  }
}

object MetadataActorSpec extends Specification with FutureMatchers with Mockito {
  implicit val system = ActorSystem("shardMetadataTest")
  implicit val timeout = Timeout(30000) 

  "shard metadata actor" should {
    "correctly propagates initial message clock on flush request" in {
      val storage = new TestMetadataStorage(Map())
      val coord = mock[CheckpointCoordination]

      val actorRef = TestActorRef(new MetadataActor("test", storage, coord, Some(YggCheckpoint(0, VectorClock.empty))))
      
      (actorRef ? FlushMetadata) must whenDelivered {
        beLike {
          case _ =>
            there was one(coord).saveYggCheckpoint("test", YggCheckpoint(0, VectorClock.empty))
        }
      }
    }

    "correctly propagates updated message clock on flush request" in {
      val storage = new TestMetadataStorage(Map())
      val coord = mock[CheckpointCoordination]

      val actorRef = TestActorRef(new MetadataActor("test", storage, coord, None))

      val colDesc = ColumnDescriptor(Path("/"), JPath(".test"), CString, Authorities(Set("me")))

      val descriptor = ProjectionDescriptor(1, colDesc :: Nil)
      val values = Vector[CValue](CString("Test123"))
      val metadata = Vector(Set[Metadata]())

      val row1 = ProjectionInsert.Row(EventId(0,1), values, metadata)
      val row2 = ProjectionInsert.Row(EventId(0,2), values, metadata)

      actorRef ! IngestBatchMetadata(Seq(descriptor -> Option(ProjectionMetadata.columnMetadata(descriptor, Seq(row1, row2)))), VectorClock.empty.update(0, 1).update(0, 2), Some(0l))
      (actorRef ? FlushMetadata) must whenDelivered {
        beLike {
          case _ => 
            val stringStats = StringValueStats(2, "Test123", "Test123")
            val resultingMetadata: ColumnMetadata = Map(colDesc -> Map(stringStats.metadataType -> stringStats))

            storage.updates(descriptor).last.metadata must_== resultingMetadata
        }
      }
    }

    "return updated metadata after batch ingest" in todo
  }

  step {
    system.shutdown
  }
}

object MetadataActorStateSpec extends Specification {
  implicit val system = ActorSystem("shardMetadataTest")

  def projectionDescriptor(path: Path, selector: JPath, cType: CType, token: String) = {
    val colDesc = ColumnDescriptor(path, selector, cType, Authorities(Set(token)))
    val desc = ProjectionDescriptor(1, colDesc :: Nil)
    val metadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (colDesc -> Map[MetadataType, Metadata]())
    Map((desc -> metadata))
  }

  val token1 = "TOKEN"

  val data: Map[ProjectionDescriptor, ColumnMetadata] = {
    projectionDescriptor(Path("/abc/"), JPath(""), CBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo"), CBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo"), CString, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo.bar"), CBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo[0]"), CString, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo"), CBoolean, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo.bar"), CBoolean, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo.bar.baz.buz"), CBoolean, token1)
  }

  val rootAbc = PathRoot(Set(
    PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(""), CBoolean, token1)),
    PathField("foo", Set(
      PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CBoolean, token1)),
      PathValue(CString, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CString, token1)),
      PathField("bar", Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo.bar"), CBoolean, token1))
      )),
      PathIndex(0, Set(
        PathValue(CString, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo[0]"), CString, token1))
      ))
    ))
  ))

  val rootDef = PathRoot(Set(
    PathField("foo", Set(
      PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def/"), JPath(".foo"), CBoolean, token1)),
      PathField("bar", Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def"), JPath(".foo.bar"), CBoolean, token1)),
        PathField("baz", Set(
          PathField("buz", Set(
            PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), CBoolean, token1))
          ))
        ))
      ))
    ))
  ))

  val source = new TestMetadataStorage(data)
  val actor = TestActorRef(new MetadataActor("test", source, CheckpointCoordination.Noop, None)).underlyingActor

  def dump(root: PathRoot, indent: Int = 0) {
    dumpMeta(root.children, indent)
  }

  def dumpMeta(meta: Set[PathMetadata], indent: Int = 0) { 
    val prefix = "  " * indent
    def log(m: String) = println(prefix + m)
    meta foreach {
      case PathValue(t, a, m) =>
        log("Value: " + t + " " + m.size)
      case PathField(n, c) =>
        log("Name " + n)
        dumpMeta(c, indent + 1)
      case PathIndex(i, c) =>
        log("Index " + i)
        dumpMeta(c, indent + 1)
    }
  }

  "local metadata state" should {
    "query by path with root selector" in {
      val result = source.findPathMetadata(Path("/abc/"), JPath("")).unsafePerformIO
    
      result must_== rootAbc
    }

    "query other path with root selector" in {
      val result = source.findPathMetadata(Path("/def/"), JPath("")).unsafePerformIO
      
      result must_== rootDef
    }

    "query by path with branch selector" in {
      val result = source.findPathMetadata(Path("/abc/"), JPath(".foo")).unsafePerformIO
     
      val expected = PathRoot(Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CBoolean, token1)),
        PathValue(CString, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CString, token1)),
        PathField("bar", Set(
          PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo.bar"), CBoolean, token1))
        )),
        PathIndex(0, Set(
          PathValue(CString, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo[0]"), CString, token1))
        ))
      ))

      result must_== expected 
    }

    "query other path with branch selector" in {
      val result = source.findPathMetadata(Path("/def/"), JPath(".foo")).unsafePerformIO
     
      val expected = PathRoot(Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def/"), JPath(".foo"), CBoolean, token1)),
        PathField("bar", Set(
          PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def"), JPath(".foo.bar"), CBoolean, token1)),
          PathField("baz", Set(
            PathField("buz", Set(
              PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), CBoolean, token1))
            ))
          ))
        ))
      ))

      result must_== expected 
    }

    "query by path with array selector" in {
      val result = source.findPathMetadata(Path("/abc/"), JPath(".foo[0]")).unsafePerformIO
     
      val expected = PathRoot(Set(
        PathValue(CString, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo[0]"), CString, token1))
      ))

      result must_== expected
    }

    "query other path with leaf selector" in {
      val result = source.findPathMetadata(Path("/def/"), JPath(".foo.bar.baz.buz")).unsafePerformIO
     
      val expected = PathRoot(Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), CBoolean, token1))
      ))

      result must_== expected 
    }
  }

  "helper methods" should {
    val colDesc1 = ColumnDescriptor(Path("/"), JPath(".foo"), CLong, Authorities(Set()))
    val descriptor1 = ProjectionDescriptor(1, colDesc1 :: Nil)

    def emptyProjections = Map[ProjectionDescriptor, ColumnMetadata]()

    "add initial metadata for the first value inserted" in {
      val value = CLong(10)
   
      val valueStats = ProjectionMetadata.valueStats(value).get
      val expectedMetadata = Map((colDesc1 -> Map[MetadataType, Metadata]((valueStats.metadataType, valueStats))))
      
      val result = ProjectionMetadata.columnMetadata(descriptor1, List(value), List(Set())) 

      result must_== expectedMetadata
    }

    "update existing metadata for values other than the first inserted" in {
      val initialValue = CLong(10)
   
      val initialValueStats = ProjectionMetadata.valueStats(initialValue).get
      val initialMetadata = Map[MetadataType, Metadata]((initialValueStats.metadataType -> initialValueStats))
      val initialColumnMetadata = Map[ColumnDescriptor, MetadataMap]((colDesc1 -> initialMetadata))
      
      val value = CLong(20)
   
      val valueStats = ProjectionMetadata.valueStats(value).flatMap{ _.merge(initialValueStats) }.get

      val expectedMetadata = Map(colDesc1 -> Map[MetadataType, Metadata](valueStats.metadataType -> valueStats))
     
      val result = ProjectionMetadata.columnMetadata(descriptor1, List(value), List(Set())) |+| initialColumnMetadata
        
      result must_== expectedMetadata
    }

    "metadata is correctly combined" in {
      val firstValue = CLong(10)
      val firstValueStats = ProjectionMetadata.valueStats(firstValue).get
      val firstMetadata = Map[MetadataType, Metadata](firstValueStats.metadataType -> firstValueStats)
      val firstColumnMetadata = Map(colDesc1 -> firstMetadata)

      val secondValue = CLong(20)
   
      val secondValueStats = ProjectionMetadata.valueStats(secondValue).get
      val secondMetadata = Map[MetadataType, Metadata]((secondValueStats.metadataType -> secondValueStats))
      val secondColumnMetadata = Map[ColumnDescriptor, MetadataMap]((colDesc1 -> secondMetadata))

      val result = secondColumnMetadata |+| firstColumnMetadata

      val expectedStats = firstValueStats.merge(secondValueStats).get
      val expected = Map((colDesc1 -> Map[MetadataType, Metadata]((expectedStats.metadataType -> expectedStats))))

      result must_== expected
    }
  }
}
