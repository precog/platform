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
package com.precog.muspelheim

import com.precog.bytecode.{JType, JObjectFixedT, JTextT}
import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.vfs._
import com.precog.util._
import SValue._
import ResourceError._

import blueeyes.json._

import akka.actor._
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import java.io._
import org.reflections._
import scalaz._
import scalaz.effect.IO
import scalaz.std.AllInstances._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

import TableModule._

trait RawJsonStorageModule[M[+_]] { self =>
  implicit def M: Monad[M]

  protected def projectionData(path: Path) = {
    if (!projections.contains(path)) load(path)
    projections(path)
  }

  private implicit val ordering = IdentitiesOrder.toScalaOrdering

  private val identity = new java.util.concurrent.atomic.AtomicInteger(0)
  private var projections: Map[Path, List[JValue]] = Map() 
  private var structures: Map[Path, Set[ColumnRef]] = Map.empty

  private def load(path: Path) = {
    val resourceName = ("/test_data" + path.toString.init + ".json").replaceAll("/+", "/")   

    using(getClass.getResourceAsStream(resourceName)) { in =>
      // FIXME: Refactor as soon as JParser can parse from InputStreams
      val reader = new InputStreamReader(in)
      val buffer = new Array[Char](8192)
      val builder = new java.lang.StringBuilder
      var read = 0
      do {
        read = reader.read(buffer)
        if (read >= 0) {
          builder.append(buffer, 0, read)
        }
      } while (read >= 0)
      
      val json = JParser.parse(builder.toString) --> classOf[JArray]

      projections += (path -> json.elements)

      val structure: Set[ColumnRef] = json.elements.foldLeft(Map.empty[ColumnRef, ArrayColumn[_]]) { (acc, jv) =>
        Slice.withIdsAndValues(jv, acc, 0, 1)
      }.keySet
      structures += (path -> structure)
    }
  }

  import org.reflections.util._
  import org.reflections.scanners._
  import scala.collection.JavaConverters._
  import java.util.regex.Pattern

  val reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("test_data")).setScanners(new ResourcesScanner()))
  val jsonFiles = reflections.getResources(Pattern.compile(".*\\.json"))
  for (resource <- jsonFiles.asScala) load(Path(resource.replaceAll("test_data/", "").replaceAll("\\.json", "")))

  val vfs: VFSMetadata[M] = new VFSMetadata[M] {
    def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[Path]] = EitherT.right {
      M.point(projections.keySet.filter(_.isDirectChildOf(path)))
    }

    def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = EitherT.right {
      M.point {
        val structs = structures.getOrElse(path, Set.empty[ColumnRef])
        val types : Map[CType, Long] = structs.collect {
          // FIXME: This should use real counts
          case ColumnRef(selector, ctype) if selector.hasPrefix(selector) => (ctype, 0L)
        }.groupBy(_._1).map { case (tpe, values) => (tpe, values.map(_._2).sum) }

        PathStructure(types, structs.map(_.selector))
      }
    }

    def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long] = {
      EitherT.right(M.point(0l))
    }
  }
}

trait RawJsonColumnarTableStorageModule[M[+_]] extends RawJsonStorageModule[M] with ColumnarTableModuleTestSupport[M] {
  import trans._
  import TableModule._

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize = UnknownSize) = new Table(slices, size)
    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = sys.error("Feature not implemented in test stub.")
    // FIXME: There should be some way to make SingletonTable work here, too
    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[M, Slice], ExactSize(1))
  }
  
  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = sys.error("Feature not implemented in test stub")
    
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("Feature not implemented in test stub.")

    def load(apiKey: APIKey, tpe: JType) = EitherT.right {
      val pathsM = this.reduce {
        new CReducer[Set[Path]] {
          def reduce(schema: CSchema, range: Range): Set[Path] = {
            schema.columns(JObjectFixedT(Map("value" -> JTextT))) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _ => Set()
            }
          }
        }
      }

      for (paths <- pathsM) yield {
        fromJson(paths.toList.map(projectionData).flatten.toStream)
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
