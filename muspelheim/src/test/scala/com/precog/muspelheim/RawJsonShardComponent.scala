package com.precog.muspelheim

import akka.actor._
import akka.dispatch._
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import java.io._
import org.reflections._
import scalaz.std.AllInstances._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

trait RawJsonShardComponent extends YggShardComponent {
  type Dataset[α]

  def actorSystem: ActorSystem 
  implicit def asyncContext: ExecutionContext

  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]): Dataset[Seq[CValue]]

  trait Storage extends YggShard[Dataset] {
    implicit val ordering = IdentitiesOrder.toScalaOrdering
    def routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

    case class DummyProjection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends Projection[Dataset] {
      val chunkSize = 2000

      def insert(ids: Identities, values: Seq[CValue], shouldSync: Boolean = false) = copy(data = data + row)

      def getAllPairs(expiresAt: Long): Dataset[Seq[CValue]] = dataset(1, data)

      def close() = ()
    }

    private val identity = new java.util.concurrent.atomic.AtomicInteger(0)
    private var projections: Map[ProjectionDescriptor, DummyProjection] = Map() 

    private def load(path: Path) = {
      val resourceName = ("/test_data" + path.toString.init + ".json").replaceAll("/+", "/")   
      using(getClass.getResourceAsStream(resourceName)) { in =>
        val reader = new InputStreamReader(in)
        val json = JsonParser.parse(reader) --> classOf[JArray]

        projections = json.elements.foldLeft(projections) { 
          case (acc, jobj) => 
            routingTable.route(EventMessage(EventId(0, identity.getAndIncrement), Event(path, "", jobj, Map()))).foldLeft(acc) {
              case (acc, ProjectionData(descriptor, identities, values, _)) =>
                acc + (descriptor -> (acc.getOrElse(descriptor, DummyProjection(descriptor, new TreeMap())) + ((identities, values))))
          }
        }
      }
    }

    def storeBatch(ems: Seq[EventMessage], timeout: Timeout) = sys.error("Feature not implemented in test stub.")

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

    def metadata = {
      val localMetadata = new LocalMetadata(projectionMetadata, VectorClock.empty)
      localMetadata.toStorageMetadata(actorSystem.dispatcher)
    }

    def userMetadataView(uid: String) = new UserMetadataView(uid, new UnlimitedAccessControl(), metadata)(actorSystem.dispatcher)

    def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection[Dataset]] = {
      Future {
        if (!projections.contains(descriptor)) descriptor.columns.map(_.path).distinct.foreach(load)
        projections(descriptor)
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
