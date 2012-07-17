package com.precog.yggdrasil

import actor._
import metadata._
import util._
import com.precog.util._
import SValue._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scalaz.effect._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

trait StubStorageModule extends StorageModule {
  type TestDataset

  def actorSystem: ActorSystem 
  implicit def asyncContext: ExecutionContext
  implicit def messagedispatcher: MessageDispatcher = MessageDispatcher.defaultDispatcher(actorSystem)

  val dataPath = Path("/test")
  def sampleSize: Int
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]): TestDataset

  // TODO: This duplicates the same class in com.precog.muspelheim.RawJsonShardComponent
  case class Projection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends FullProjectionLike[TestDataset] {
    val chunkSize = 2000

    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Dummy ProjectionLike doesn't support insert")      

    def allRecords(expiresAt: Long): TestDataset = dataset(1, data)
  }

  class Storage extends StorageLike[Projection] {
    implicit val ordering = IdentitiesOrder.toScalaOrdering
    def routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
    
    lazy val sampleData: Vector[JValue] = DistributedSampleSet.sample(sampleSize, 0)._1

    val projections: Map[ProjectionDescriptor, Projection] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, Projection]) { 
      case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
        case (acc, ProjectionData(descriptor, values, _)) =>
          acc + (descriptor -> (Projection(descriptor, acc.get(descriptor).map(_.data).getOrElse(TreeMap.empty(ordering)) + (VectorCase(EventId(0,i).uid) -> values))))
      }
    }

    def storeBatch(ems: Seq[EventMessage], timeout: Timeout) = sys.error("Feature not implemented in test stub.")

    def projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = 
      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap

    lazy val metadataActor = {
      implicit val system = actorSystem
      TestActorRef(new MetadataActor("JSONTest", new TestMetadataStorage(projectionMetadata), CheckpointCoordination.Noop, None))
    }

    def metadata = new ActorStorageMetadata(metadataActor)

    def userMetadataView(uid: String) = new UserMetadataView(uid, new UnlimitedAccessControl(), metadata)(actorSystem.dispatcher)

    def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(Projection, Release)] =
      Future((projections(descriptor), new Release(IO(()))))
  }
}



// vim: set ts=4 sw=4 et:
