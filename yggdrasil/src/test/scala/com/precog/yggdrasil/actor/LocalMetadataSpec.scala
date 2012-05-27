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

import metadata.ColumnMetadata._

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil.metadata._
import com.precog.util.VectorClock

import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap
import scalaz.{Success, Validation}
import scalaz.effect._
import scalaz.syntax.monoid._
import scalaz.syntax.std.optionV._

import blueeyes.json.JPath

class MetadataActorStateSpec extends Specification {
  def projectionDescriptor(path: Path, selector: JPath, cType: CType, token: String) = {
    val colDesc = ColumnDescriptor(path, selector, cType, Authorities(Set(token)))
    val desc = ProjectionDescriptor(ListMap() + (colDesc -> 0), List[(ColumnDescriptor, SortBy)]() :+ (colDesc, ById)).toOption.get
    val metadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (colDesc -> Map[MetadataType, Metadata]())
    Map((desc -> metadata))
  }

  val token1 = "TOKEN"

  val data: Map[ProjectionDescriptor, ColumnMetadata] = {
    projectionDescriptor(Path("/abc/"), JPath(""), CBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo"), CBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo"), CStringArbitrary, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo.bar"), CBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo[0]"), CStringArbitrary, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo"), CBoolean, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo.bar"), CBoolean, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo.bar.baz.buz"), CBoolean, token1)
  }

  val rootAbc = PathRoot(Set(
    PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(""), CBoolean, token1)),
    PathField("foo", Set(
      PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CBoolean, token1)),
      PathValue(CStringArbitrary, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CStringArbitrary, token1)),
      PathField("bar", Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo.bar"), CBoolean, token1))
      )),
      PathIndex(0, Set(
        PathValue(CStringArbitrary, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo[0]"), CStringArbitrary, token1))
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

  val state = new MetadataActor.State(new TestMetadataStorage(data), VectorClock.empty, None) 

  "local metadata state" should {
    "query by path with root selector" in {
      val result = state.findPathMetadata(Path("/abc/"), JPath(""))
    
      result must_== rootAbc
    }

    "query other path with root selector" in {
      val result = state.findPathMetadata(Path("/def/"), JPath(""))
      
      result must_== rootDef
    }

    "query by path with branch selector" in {
      val result = state.findPathMetadata(Path("/abc/"), JPath(".foo"))
     
      val expected = PathRoot(Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CBoolean, token1)),
        PathValue(CStringArbitrary, Authorities(Set(token1)), projectionDescriptor(Path("/abc/"), JPath(".foo"), CStringArbitrary, token1)),
        PathField("bar", Set(
          PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo.bar"), CBoolean, token1))
        )),
        PathIndex(0, Set(
          PathValue(CStringArbitrary, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo[0]"), CStringArbitrary, token1))
        ))
      ))

      result must_== expected 
    }

    "query other path with branch selector" in {
      val result = state.findPathMetadata(Path("/def/"), JPath(".foo"))
     
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
      val result = state.findPathMetadata(Path("/abc/"), JPath(".foo[0]"))
     
      val expected = PathRoot(Set(
        PathValue(CStringArbitrary, Authorities(Set(token1)), projectionDescriptor(Path("/abc"), JPath(".foo[0]"), CStringArbitrary, token1))
      ))

      result must_== expected
    }

    "query other path with leaf selector" in {
      val result = state.findPathMetadata(Path("/def/"), JPath(".foo.bar.baz.buz"))
     
      val expected = PathRoot(Set(
        PathValue(CBoolean, Authorities(Set(token1)), projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), CBoolean, token1))
      ))

      result must_== expected 
    }
  }

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

  "helper methods" should {
    val colDesc1 = ColumnDescriptor(Path("/"), JPath(".foo"), CInt, Authorities(Set()))
    val descriptor1 = ProjectionDescriptor(ListMap[ColumnDescriptor, Int]((colDesc1 -> 0)), Seq[(ColumnDescriptor, SortBy)]((colDesc1 -> ById))).toOption.get

    def emptyProjections = Map[ProjectionDescriptor, ColumnMetadata]()

    "add initial metadata for the first value inserted" in {
      val value = CInt(10)
   
      val valueStats = ProjectionMetadata.valueStats(value).get
      val expectedMetadata = Map((colDesc1 -> Map[MetadataType, Metadata]((valueStats.metadataType, valueStats))))
      
      val result = ProjectionMetadata.columnMetadata(descriptor1, List(value), List(Set())) 

      result must_== expectedMetadata
    }

    "update existing metadata for values other than the first inserted" in {
      val initialValue = CInt(10)
   
      val initialValueStats = ProjectionMetadata.valueStats(initialValue).get
      val initialMetadata = Map[MetadataType, Metadata]((initialValueStats.metadataType -> initialValueStats))
      val initialColumnMetadata = Map[ColumnDescriptor, MetadataMap]((colDesc1 -> initialMetadata))
      
      val value = CInt(20)
   
      val valueStats = ProjectionMetadata.valueStats(value).flatMap{ _.merge(initialValueStats) }.get

      val expectedMetadata = Map(colDesc1 -> Map[MetadataType, Metadata](valueStats.metadataType -> valueStats))
     
      val result = ProjectionMetadata.columnMetadata(descriptor1, List(value), List(Set())) |+| initialColumnMetadata
        
      result must_== expectedMetadata
    }

    "metadata is correctly combined" in {
      val firstValue = CInt(10)
      val firstValueStats = ProjectionMetadata.valueStats(firstValue).get
      val firstMetadata = Map[MetadataType, Metadata](firstValueStats.metadataType -> firstValueStats)
      val firstColumnMetadata = Map(colDesc1 -> firstMetadata)

      val secondValue = CInt(20)
   
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
