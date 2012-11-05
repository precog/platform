package com.precog.yggdrasil

import actor._
import metadata._
import util._
import com.precog.util._
import SValue._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._
import com.precog.common.json._

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


trait StubStorageModule[M[+_]] extends StorageModule[M] { self =>
  type TestDataset

  implicit def M: Monad[M]

  def projections: Map[ProjectionDescriptor, Projection]

  class Storage extends StorageLike {
    def storeBatch(ems: Seq[EventMessage]) = sys.error("Feature not implemented in test stub.")

    def projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = 
      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap

    def metadata = new StorageMetadata[M] {
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

    def userMetadataView(uid: String) = new UserMetadataView[M](uid, new UnlimitedAccessControl(), metadata)

    def projection(descriptor: ProjectionDescriptor) = M.point(projections(descriptor) -> new Release(IO(())))
  }
}


trait DistributedSampleStubStorageModule[M[+_]] extends StubStorageModule[M] {
  val dataPath = Path("/test")
  def sampleSize: Int
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]): TestDataset

  // TODO: This duplicates the same class in com.precog.muspelheim.RawJsonShardComponent
  case class Projection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends ProjectionLike {
    val chunkSize = 2000

    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit = sys.error("Dummy ProjectionLike doesn't support insert")      
    def commit(): IO[Unit] = sys.error("Dummy ProjectionLike doesn't support commit")
  }

  implicit lazy val ordering = IdentitiesOrder.toScalaOrdering

  lazy val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

  lazy val sampleData: Vector[JValue] = DistributedSampleSet.sample(sampleSize, 0)._1

  lazy val projections: Map[ProjectionDescriptor, Projection] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, Projection]) { 
    case (acc, (jobj, i)) => routingTable.routeEvent(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
      case (acc, ProjectionData(descriptor, values, _)) =>
        acc + (descriptor -> (Projection(descriptor, acc.get(descriptor).map(_.data).getOrElse(TreeMap.empty(ordering)) + (Array(EventId(0,i).uid) -> values))))
    }
  }
}


// vim: set ts=4 sw=4 et:
