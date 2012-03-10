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

trait StubYggShardComponent {
  def actorSystem: ActorSystem 
  implicit def asyncContext: ExecutionContext

  val dataPath = Path("/test")
  def sampleSize: Int

  trait Storage extends YggShard {

    def routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

    case class DummyProjection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends Projection {
      val chunkSize = 2000

      def + (row: (Identities, Seq[CValue])) = copy(data = data + row)

      def getAllPairs(expiresAt: Long): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = {
        enumPStream[X, Vector[(Identities, Seq[CValue])], IO](data.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getAllValues(expiresAt: Long): EnumeratorP[X, Vector[Seq[CValue]], IO] = {
        enumPStream[X, Vector[Seq[CValue]], IO](data.values.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getAllIds(expiresAt: Long): EnumeratorP[X, Vector[Identities], IO] = {
        enumPStream[X, Vector[Identities], IO](data.keys.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getAllColumnPairs(columnIndex: Int, expiresAt: Long) : EnumeratorP[X, Vector[(Identities, CValue)], IO] = {
        enumPStream[X, Vector[(Identities, CValue)], IO](data.map{case (i,v) => (i, v(columnIndex))}.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getPairsByIdRange(range: Interval[Identities], expiresAt: Long): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = sys.error("not needed")
    }

    val (sampleData, _) = DistributedSampleSet.sample(sampleSize, 0)

    val projections: Map[ProjectionDescriptor, Projection] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, DummyProjection]) { 
      case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
        case (acc, ProjectionData(descriptor, identities, values, _)) =>
          acc + (descriptor -> (acc.getOrElse(descriptor, DummyProjection(descriptor, new TreeMap())) + ((identities, values))))
      }
    }

    def storeBatch(ems: Seq[EventMessage], timeout: Timeout) = sys.error("Feature not implemented in test stub.")

    def metadata = new StorageMetadata {
      implicit val dispatcher = actorSystem.dispatcher
      def findSelectors(path: Path): Future[Seq[JPath]] = 
        Future(projections.keys.flatMap(_.columns.filter(_.path == path).map(_.selector)).toSeq)

      def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]] = 
        Future(projections.keys.flatMap(pd => pd.columns.collect { case cd @ ColumnDescriptor(`path`, `selector`, _, _) => (pd, ColumnMetadata.Empty) }).toMap)
    }

    def userMetadataView(uid: String) = new UserMetadataView(uid, UnlimitedAccessControl, metadata)(actorSystem.dispatcher)

    def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection] =
      Future(projections(descriptor))
  }
}



// vim: set ts=4 sw=4 et:
