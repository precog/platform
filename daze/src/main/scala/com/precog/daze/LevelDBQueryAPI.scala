package com.precog
package daze

import scala.annotation.tailrec

import com.precog.yggdrasil._
import com.precog.yggdrasil.kafka._
import com.precog.yggdrasil.leveldb._
import com.precog.yggdrasil.shard._
import com.precog.analytics.Path
import StorageMetadata._

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.duration._
import blueeyes.json.JPath
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.set._
import scalaz.std.AllInstances._
import Iteratee._

trait LevelDBQueryConfig {
  def projectionRetrievalTimeout: akka.util.Timeout
}

trait LevelDBQueryComponent extends YggConfigComponent with StorageEngineQueryComponent {
  type YggConfig <: LevelDBQueryConfig

  def storage: YggShard

  trait QueryAPI extends StorageEngineQueryAPI {
    override def fullProjection[X](path: Path)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = DatasetEnum(
      for {
        selectors   <- storage.metadata.findSelectors(path) 
        sources     <- Future.sequence(selectors.map(s => storage.metadata.findProjections(path, s).map(p => (s, p))))
        enumerator: EnumeratorP[X, Vector[SEvent], IO]  <- assemble(path, sources)
      } yield enumerator
    )

    override def mask[X](path: Path): DatasetMask[X] = LevelDBDatasetMask[X](path, None, None) 

    private case class LevelDBDatasetMask[X](path: Path, selector: Option[JPath], tpe: Option[SType]) extends DatasetMask[X] {
      def derefObject(field: String): DatasetMask[X] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ field })

      def derefArray(index: Int): DatasetMask[X] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ index })

      def typed(tpe: SType): DatasetMask[X] = copy(tpe = Some(tpe))

      def realize(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
        def assembleForSelector(selector: JPath, retrieval: Future[Map[ProjectionDescriptor, ColumnMetadata]]) = 
          DatasetEnum(
            for {
              descriptors <- retrieval 
              enum        <- assemble[X](path, List((selector, descriptors))) 
            } yield {
              enum map { _ map { case (ids, sv) => (sv \ selector) map { v => (ids, v) } } collect { case Some(t) => t } }
            }
          )

        (selector, tpe) match {
          case (Some(s), Some(tpe)) => assembleForSelector(s, storage.metadata.findProjections(path, s, tpe))
          case (Some(s), None     ) => assembleForSelector(s, storage.metadata.findProjections(path, s))
          case (None   , Some(tpe)) => assembleForSelector(JPath.Identity, storage.metadata.findProjections(path, JPath.Identity))
          case (_      , _        ) => fullProjection(path)
        }
      }
    }

    def assemble[X](path: Path, sources: Seq[(JPath, Map[ProjectionDescriptor, ColumnMetadata])])(implicit asyncContext: ExecutionContext): Future[EnumeratorP[X, Vector[SEvent], IO]] = {
      def retrieveAndMerge[X](path: Path, selector: JPath, descriptors: Set[ProjectionDescriptor]): Future[EnumeratorP[X, Vector[SColumn], IO]] = {
        for {
          projections <- Future.sequence(descriptors map { storage.projection(_)(yggConfig.projectionRetrievalTimeout) })
        } yield {
          EnumeratorP.mergeAll(projections.map(_.getColumnValues[X](path, selector)).toSeq: _*)
        }
      }

      // determine the projections from which to retrieve data
      // todo: for right now, this is implemented greedily such that the first
      // projection containing a desired column wins. It should be implemented
      // to choose the projection that satisfies the largest number of columns.
      val descriptors = sources.foldLeft(Map.empty[JPath, Set[ProjectionDescriptor]]) {
        case (acc, (selector, descriptorData)) => descriptorData.foldLeft(acc) {
          case (acc, (descriptor, _)) => 
            acc.get(selector) match {
              case Some(chosen) if chosen.contains(descriptor) ||
                                   (chosen exists { d => descriptor.columnAt(path, selector).exists(d.satisfies) }) => acc

              case _ => acc + (selector -> (acc.getOrElse(selector, Set.empty[ProjectionDescriptor]) + descriptor))
            }
        }
      }

      // now use merge with identity ordering to produce an enumerator for each selector. Given identity ordering, 
      // encountering the middle case is an error since no single identity should ever correspond to 
      // two values of different types, so the resulting merged set should not contain any duplicate identities.
      val mergedFutures = descriptors map {
        case (selector, descriptors) => retrieveAndMerge(path, selector, descriptors).map((e: EnumeratorP[X, Vector[(Identities, CValue)], IO]) => (selector, e))
      }

      implicit val SEventOrder = SEventIdentityOrder

      Future.sequence(mergedFutures) map { en => combine[X](en.toList) }
    }

    def combine[X](enumerators: List[(JPath, EnumeratorP[X, Vector[SColumn], IO])])(implicit o: Order[SEvent]): EnumeratorP[X, Vector[SEvent], IO] = {
      def combine(enumerators: List[(JPath, EnumeratorP[X, Vector[SColumn], IO])]): EnumeratorP[X, Vector[SEvent], IO] = {
        enumerators match {
          case (selector, column) :: xs => 
            cogroupE[X, SEvent, SColumn, IO].apply(combine(xs), column).map { _ map {
              case Left3(sevent) => sevent
              case Middle3(((id, svalue), (_, cv))) => (id, svalue.set(selector, cv).getOrElse(sys.error("Cannot reassemble object: conflicting values for " + selector)))
              case Right3((id, cv)) => (id, SValue(selector, cv))
            } }
          case Nil => EnumeratorP.empty[X, Vector[SEvent], IO]
        }
      }

      combine(enumerators)
    }
  }
}
// vim: set ts=4 sw=4 et:
