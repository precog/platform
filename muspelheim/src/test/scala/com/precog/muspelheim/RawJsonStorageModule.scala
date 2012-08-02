package com.precog.muspelheim

import com.precog.bytecode.JType
import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

import akka.actor._
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

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

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

trait RawJsonStorageModule[M[+_]] extends StorageModule[M] { self =>
  implicit def M: Monad[M]

  trait ProjectionCompanion {
    def apply(descriptor: ProjectionDescriptor, data: Vector[JValue]): Projection
  }

  val Projection: ProjectionCompanion

  trait Storage extends StorageLike {
    implicit val ordering = IdentitiesOrder.toScalaOrdering
    def routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

    private val identity = new java.util.concurrent.atomic.AtomicInteger(0)
    private var projections: Map[ProjectionDescriptor, Vector[JValue]] = Map() 

    private def load(path: Path) = {
      val resourceName = ("/test_data" + path.toString.init + ".json").replaceAll("/+", "/")   
      using(getClass.getResourceAsStream(resourceName)) { in =>
        val reader = new InputStreamReader(in)
        val json = JsonParser.parse(reader) --> classOf[JArray]

        projections = json.elements.foldLeft(projections) { 
          case (acc, jobj) => 
            val evID = EventId(0, identity.getAndIncrement)
            routingTable.route(EventMessage(evID, Event(path, "", jobj, Map()))).foldLeft(acc) {
              case (acc, data) =>
                acc + (data.descriptor -> (acc.getOrElse(data.descriptor, Vector.empty[JValue]) :+ data.toJValue))
          }
        }
      }
    }

    def storeBatch(ems: Seq[EventMessage]) = sys.error("Feature not implemented in test stub.")

    lazy val projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = {
      import org.reflections.util._
      import org.reflections.scanners._
      import scala.collection.JavaConverters._
      import java.util.regex.Pattern

      val reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("test_data")).setScanners(new ResourcesScanner()));
      val jsonFiles = reflections.getResources(Pattern.compile(".*\\.json"))
      for (resource <- jsonFiles.asScala) load(Path(resource.replaceAll("test_data/", "").replaceAll("\\.json", "")))

      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap
    }

    def metadata = new StorageMetadata[M] {
      val M = self.M
      val source = new TestMetadataStorage(projectionMetadata)
      def findChildren(path: Path) = M.point(source.findChildren(path))
      def findSelectors(path: Path) = M.point(source.findSelectors(path))
      def findProjections(path: Path, selector: JPath) = M.point {
        projections.collect {
          case (descriptor, _) if descriptor.columns.exists { case ColumnDescriptor(p, s, _, _) => p == path && s == selector } => 
            (descriptor, ColumnMetadata.Empty)
        }
      }

      def findPathMetadata(path: Path, selector: JPath) = M.point(source.findPathMetadata(path, selector).unsafePerformIO)
    }

    def userMetadataView(uid: String) = new UserMetadataView(uid, new UnlimitedAccessControl[M](), metadata)

    def projection(descriptor: ProjectionDescriptor): M[(Projection, Release)] = {
      M.point {
        if (!projections.contains(descriptor)) descriptor.columns.map(_.path).distinct.foreach(load)
        (Projection(descriptor, projections(descriptor)), new Release(scalaz.effect.IO(())))
      }
    }
  }
}

trait RawJsonColumnarTableStorageModule[M[+_]] extends RawJsonStorageModule[M] with ColumnarTableModule[M] {
  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    def load(uid: UserId, tpe: JType): M[Table] = {
      sys.error("todo")
    }
  }

  class Projection(val descriptor: ProjectionDescriptor, data: Vector[JValue]) extends ProjectionLike {
    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("DummyProjection doesn't support insert")
  }

  object Projection extends ProjectionCompanion {
    def apply(descriptor: ProjectionDescriptor, data: Vector[JValue]): Projection = sys.error("todo")
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)

  object storage extends Storage
}


// vim: set ts=4 sw=4 et:
