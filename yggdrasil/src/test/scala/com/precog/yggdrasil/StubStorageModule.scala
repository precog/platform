package com.precog.yggdrasil

import actor._
import metadata._
import util._
import SValue._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.util._
import com.precog.common.json._
import com.precog.util._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json._

import scalaz._
import scalaz.effect._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class StubStorageMetadata[M[+_]](projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata])(implicit val M: Monad[M]) extends StorageMetadata[M] {
  val source = new TestMetadataStorage(projectionMetadata)
  def findChildren(path: Path) = M.point(source.findChildren(path))
  def findSelectors(path: Path) = M.point(source.findSelectors(path))
  def findProjections(path: Path, selector: CPath) = M.point {
    projectionMetadata.collect {
      case (descriptor, _) if descriptor.columns.exists { case ColumnDescriptor(p, s, _, _) => p == path && s == selector } => 
        (descriptor, ColumnMetadata.Empty)
    }
  }

  def findPathMetadata(path: Path, selector: CPath) = M.point(source.findPathMetadata(path, selector).unsafePerformIO)
}

trait StubStorageModule[M[+_]] extends StorageModule[M] { self =>
  type TestDataset

  implicit def M: Monad[M]

  def projections: Map[ProjectionDescriptor, Projection]

  class Storage extends StorageLike[M] {
    def storeBatch(ems: Seq[EventMessage]) = sys.error("Feature not implemented in test stub.")

    def projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = 
      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap

    def metadata = new StubStorageMetadata(projectionMetadata)(M)

    def userMetadataView(apiKey: APIKey) = new UserMetadataView[M](apiKey, new UnrestrictedAccessControl(), metadata)

    def projection(descriptor: ProjectionDescriptor) = M.point(projections(descriptor) -> new Release(IO(PrecogUnit)))
  }
}


trait DistributedSampleStubStorageModule[M[+_]] extends StubStorageModule[M] {
  import ProjectionInsert.Row
  val dataPath = Path("/test")
  def sampleSize: Int
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]): TestDataset

  // TODO: This duplicates the same class in com.precog.muspelheim.RawJsonShardComponent
  case class Projection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends ProjectionLike {
    val chunkSize = 2000

    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit = sys.error("Dummy ProjectionLike doesn't support insert")      
    def commit(): IO[PrecogUnit] = sys.error("Dummy ProjectionLike doesn't support commit")
  }

  implicit lazy val ordering = IdentitiesOrder.toScalaOrdering

  lazy val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

  lazy val sampleData: Vector[JValue] = DistributedSampleSet.sample(sampleSize, 0)._1

  lazy val projections: Map[ProjectionDescriptor, Projection] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, Projection]) { 
    case (acc, (jv, i)) => routingTable.routeIngest(IngestMessage("", dataPath, "", Vector(IngestRecord(EventId(0, i), jv)), None)).foldLeft(acc) {
      case (acc, ProjectionInsert(descriptor, rows)) =>
        rows.foldLeft(acc) { 
          case (acc, Row(eventId, values, _)) =>
            val data = acc.get(descriptor).map(_.data).getOrElse(TreeMap.empty(ordering)) + (Array(eventId.uid) -> values)
            acc + (descriptor -> Projection(descriptor, data))
        }
    }
  }
}


// vim: set ts=4 sw=4 et:
