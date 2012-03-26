package com.precog
package daze

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

import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.AllInstances._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

trait StubYggShardComponent extends YggShardComponent {
  def actorSystem: ActorSystem 
  implicit def asyncContext: ExecutionContext
  type Dataset[E] = IterableDataset[E]

  val dataPath = Path("/test")
  def sampleSize: Int

  trait Storage extends YggShard[Dataset] {
    implicit val ordering = IdentitiesOrder.toScalaOrdering
    def routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

    case class DummyProjection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends Projection[Dataset] {
      val chunkSize = 2000

      def + (row: (Identities, Seq[CValue])) = copy(data = data + row)

      def getAllPairs(expiresAt: Long): Dataset[Seq[CValue]] = IterableDataset(1, data.toList)
    }

    val (sampleData, _) = DistributedSampleSet.sample(sampleSize, 0)

    val projections: Map[ProjectionDescriptor, Projection[Dataset]] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, DummyProjection]) { 
      case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
        case (acc, ProjectionData(descriptor, identities, values, _)) =>
          acc + (descriptor -> (acc.getOrElse(descriptor, DummyProjection(descriptor, new TreeMap())) + ((identities, values))))
      }
    }

    def storeBatch(ems: Seq[EventMessage], timeout: Timeout) = sys.error("Feature not implemented in test stub.")

    def projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = 
      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap

    def metadata = {
      val localMetadata = new LocalMetadata(projectionMetadata, VectorClock.empty)
      localMetadata.toStorageMetadata(actorSystem.dispatcher)
    }

    def userMetadataView(uid: String) = new UserMetadataView(uid, new UnlimitedAccessControl(), metadata)(actorSystem.dispatcher)

    def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection[Dataset]] =
      Future(projections(descriptor))
  }
}



// vim: set ts=4 sw=4 et:
