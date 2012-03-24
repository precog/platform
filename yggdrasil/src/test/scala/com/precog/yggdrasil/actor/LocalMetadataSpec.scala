package com.precog.yggdrasil
package actor

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil.metadata._

import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import blueeyes.json.JPath

class LocalMetadataSpec extends Specification {
  
  def projectionDescriptor(path: Path, selector: JPath, cType: ColumnType, token: String) = {
    val colDesc = ColumnDescriptor(path, selector, cType, Authorities(Set(token)))
    val desc = ProjectionDescriptor(ListMap() + (colDesc -> 0), List[(ColumnDescriptor, SortBy)]() :+ (colDesc, ById)).toOption.get
    val metadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (colDesc -> Map[MetadataType, Metadata]())
    Map((desc -> metadata))
  }

  val token1 = "TOKEN"

  val data = {
    projectionDescriptor(Path("/abc/"), JPath(""), SBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo"), SBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo"), SStringArbitrary, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo.bar"), SBoolean, token1) ++
    projectionDescriptor(Path("/abc/"), JPath(".foo[0]"), SStringArbitrary, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo"), SBoolean, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo.bar"), SBoolean, token1) ++
    projectionDescriptor(Path("/def/"), JPath(".foo.bar.baz.buz"), SBoolean, token1)
  }

  val rootAbc = PathRoot(Set(
    PathValue(SBoolean, projectionDescriptor(Path("/abc/"), JPath(""), SBoolean, token1)),
    PathField("foo", Set(
      PathValue(SBoolean, projectionDescriptor(Path("/abc/"), JPath(".foo"), SBoolean, token1)),
      PathValue(SStringArbitrary, projectionDescriptor(Path("/abc/"), JPath(".foo"), SStringArbitrary, token1)),
      PathField("bar", Set(
        PathValue(SBoolean, projectionDescriptor(Path("/abc"), JPath(".foo.bar"), SBoolean, token1))
      )),
      PathIndex(0, Set(
        PathValue(SStringArbitrary, projectionDescriptor(Path("/abc"), JPath(".foo[0]"), SStringArbitrary, token1))
      ))
    ))
  ))

  val rootDef = PathRoot(Set(
    PathField("foo", Set(
      PathValue(SBoolean, projectionDescriptor(Path("/def/"), JPath(".foo"), SBoolean, token1)),
      PathField("bar", Set(
        PathValue(SBoolean, projectionDescriptor(Path("/def"), JPath(".foo.bar"), SBoolean, token1)),
        PathField("baz", Set(
          PathField("buz", Set(
            PathValue(SBoolean, projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), SBoolean, token1))
          ))
        ))
      ))
    ))
  ))

  val lm = new LocalMetadata(data, VectorClock.empty)

  "local metadata" should {
    "query by path with root selector" in {
      val result = lm.findPathMetadata(Path("/abc/"), JPath(""))
    
      result must_== rootAbc
    }
    "query other path with root selector" in {
      val result = lm.findPathMetadata(Path("/def/"), JPath(""))
      
      result must_== rootDef
    }
    "query by path with branch selector" in {
      val result = lm.findPathMetadata(Path("/abc/"), JPath(".foo"))
     
      val expected = PathRoot(Set(
        PathValue(SBoolean, projectionDescriptor(Path("/abc/"), JPath(".foo"), SBoolean, token1)),
        PathValue(SStringArbitrary, projectionDescriptor(Path("/abc/"), JPath(".foo"), SStringArbitrary, token1)),
        PathField("bar", Set(
          PathValue(SBoolean, projectionDescriptor(Path("/abc"), JPath(".foo.bar"), SBoolean, token1))
        )),
        PathIndex(0, Set(
          PathValue(SStringArbitrary, projectionDescriptor(Path("/abc"), JPath(".foo[0]"), SStringArbitrary, token1))
        ))
      ))

      result must_== expected 
    }
    "query other path with branch selector" in {
      val result = lm.findPathMetadata(Path("/def/"), JPath(".foo"))
     
      val expected = PathRoot(Set(
        PathValue(SBoolean, projectionDescriptor(Path("/def/"), JPath(".foo"), SBoolean, token1)),
        PathField("bar", Set(
          PathValue(SBoolean, projectionDescriptor(Path("/def"), JPath(".foo.bar"), SBoolean, token1)),
          PathField("baz", Set(
            PathField("buz", Set(
              PathValue(SBoolean, projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), SBoolean, token1))
            ))
          ))
        ))
      ))

      result must_== expected 
    }
    "query by path with array selector" in {
      val result = lm.findPathMetadata(Path("/abc/"), JPath(".foo[0]"))
     
      val expected = PathRoot(Set(
        PathValue(SStringArbitrary, projectionDescriptor(Path("/abc"), JPath(".foo[0]"), SStringArbitrary, token1))
      ))

      result must_== expected
    }
    "query other path with leaf selector" in {
      val result = lm.findPathMetadata(Path("/def/"), JPath(".foo.bar.baz.buz"))
     
      val expected = PathRoot(Set(
        PathValue(SBoolean, projectionDescriptor(Path("/def"), JPath(".foo.bar.baz.buz"), SBoolean, token1))
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
      case PathValue(t, m) =>
        log("Value: " + t + " " + m.size)
      case PathField(n, c) =>
        log("Name " + n)
        dumpMeta(c, indent + 1)
      case PathIndex(i, c) =>
        log("Index " + i)
        dumpMeta(c, indent + 1)
    }
  }
}
