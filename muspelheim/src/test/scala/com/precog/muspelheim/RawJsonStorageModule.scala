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

import com.precog.bytecode._
import com.precog.common.json.CPath
import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

import akka.actor._
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json._

import com.precog.common._
import com.precog.common.security._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

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

trait RawJsonStorageModule[M[+_]] extends StorageModule[M] { self =>
  implicit def M: Monad[M]

//  trait ProjectionCompanion {
//    def apply(descriptor: ProjectionDescriptor, data: Vector[JValue]): Projection
//  }
//
//  val Projection: ProjectionCompanion
  def projectionFor(descriptor: ProjectionDescriptor, data: Vector[JValue]): Projection

  abstract class Storage extends StorageLike {
    implicit val ordering = IdentitiesOrder.toScalaOrdering
    val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

    private val identity = new java.util.concurrent.atomic.AtomicInteger(0)
    private var projections: Map[ProjectionDescriptor, Vector[JValue]] = Map() 

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

        projections = json.elements.foldLeft(projections) { 
          case (acc, jobj) => 
            val evID = EventId(0, identity.getAndIncrement)
            routingTable.routeEvent(EventMessage(evID, Event(path, "", jobj, Map()))).foldLeft(acc) {
              case (acc, data) =>
                acc + (data.descriptor -> (acc.getOrElse(data.descriptor, Vector.empty[JValue]) :+ data.toJValue))
          }
        }
      }
    }

    def storeBatch(ems: Seq[EventMessage]) = sys.error("Feature not implemented in test stub.")

    val projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = {
      import org.reflections.util._
      import org.reflections.scanners._
      import scala.collection.JavaConverters._
      import java.util.regex.Pattern

      val reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("test_data")).setScanners(new ResourcesScanner()))
      val jsonFiles = reflections.getResources(Pattern.compile(".*\\.json"))
      for (resource <- jsonFiles.asScala) load(Path(resource.replaceAll("test_data/", "").replaceAll("\\.json", "")))

      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap
    }

    val metadata = new StorageMetadata[M] {
      val M = self.M
      val source = new TestMetadataStorage(projectionMetadata)
      def findChildren(path: Path) = M.point(source.findChildren(path))
      def findSelectors(path: Path) = M.point(source.findSelectors(path))
      def findProjections(path: Path, selector: CPath) = M.point {
        projections.collect {
          case (descriptor, _) if descriptor.columns.exists { case ColumnDescriptor(p, s, _, _) => p == path && s == selector } => 
            (descriptor, ColumnMetadata.Empty)
        }
      }

      def findPathMetadata(path: Path, selector: CPath) = M.point(source.findPathMetadata(path, selector).unsafePerformIO)
    }

    def userMetadataView(uid: String) = new UserMetadataView(uid, new UnlimitedAccessControl[M](), metadata)

    def projection(descriptor: ProjectionDescriptor): M[(Projection, Release)] = {
      M.point {
        if (!projections.contains(descriptor)) descriptor.columns.map(_.path).distinct.foreach(load)
        (projectionFor(descriptor, projections(descriptor)), new Release(scalaz.effect.IO(PrecogUnit)))
      }
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

    def load(uid: UserId, tpe: JType): M[Table] = {
      val pathsM = this.reduce {
        new CReducer[Set[Path]] {
          def reduce(columns: JType => Set[Column], range: Range): Set[Path] = {
            columns(JObjectFixedT(Map("value" -> JTextT))) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _ => Set()
            }
          }
        }
      }

      for {
        paths <- pathsM
        data  <- paths.toList.map(storage.userMetadataView("").findProjections).sequence
        path  = data.flatten.headOption
        table <- path map { 
                   case (descriptor, _) => storage.projection(descriptor) map { projection => fromJson(projection._1.data.toStream) }
                 } getOrElse {
                   M.point(Table.empty)
                 }
      } yield table
    }
  }

  class Projection(val descriptor: ProjectionDescriptor, val data: Vector[JValue]) extends ProjectionLike {
    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit = sys.error("DummyProjection doesn't support insert")
    def commit(): IO[PrecogUnit] = sys.error("DummyProjection doesn't support commit")
  }

  def projectionFor(descriptor: ProjectionDescriptor, data: Vector[JValue]): Projection = {
    println("New projection")
    new Projection(descriptor, data)
  }

  object storage extends Storage
}


// vim: set ts=4 sw=4 et:
