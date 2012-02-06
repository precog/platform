package com.precog.daze

import scala.annotation.tailrec

import com.precog.yggdrasil._
import com.precog.yggdrasil.kafka._
import com.precog.yggdrasil.leveldb._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util.Enumerators
import com.precog.analytics.Path
import StorageMetadata._

import akka.dispatch.Future
import akka.util.duration._
import blueeyes.json.JPath
import java.io.File
import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.set._
import scalaz.std.AllInstances._

import Iteratee._
trait LevelDBQueryAPI extends StorageEngineQueryAPI {
  implicit def projectionRetrievalTimeout: akka.util.Timeout = 10 seconds
  implicit def asyncContext: akka.dispatch.ExecutionContext
  def storage: StorageShard

  def fullProjection[X](path: Path): Future[DatasetEnum[X, SEvent, IO]] = {
    for {
      selectors   <- storage.metadata.findSelectors(path) 
      sources     <- Future.sequence(selectors.map(s => storage.metadata.findProjections(path, s).map(p => (s, p))))
      enumerator: EnumeratorP[X, SEvent, IO]  <- assemble(path, sources)
    } yield DatasetEnum(enumerator)
  }
  
  def mask(path: Path): DatasetMask = null

  def assemble[X](path: Path, sources: Seq[(JPath, scala.collection.Map[ProjectionDescriptor, ColumnMetadata])]): Future[EnumeratorP[X, SEvent, IO]] = {
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
      case (selector, descriptors) => retrieveAndMerge(path, selector, descriptors).map((e: EnumeratorP[X, (Identities, CValue), IO]) => (selector, e))
    }

    Future.sequence(mergedFutures) map { en => combine[X](en.toList) }
  }

  private implicit def identityOrder[A, B]: (((Identities, A), (Identities, B)) => Ordering) = 
    (t1: (Identities, A), t2: (Identities, B)) => {
      (t1._1 zip t2._1).foldLeft[Ordering](Ordering.EQ) {
        case (Ordering.EQ, (i1, i2)) => Order[Long].order(i1, i2)
        case (ord, _) => ord
      }
    }

  private implicit object SColumnIdentityOrder extends Order[SColumn] {
    def order(c1: SColumn, c2: SColumn) = identityOrder(c1, c2)
  }

  private def retrieveAndMerge[X](path: Path, selector: JPath, descriptors: Set[ProjectionDescriptor]): Future[EnumeratorP[X, SColumn, IO]] = {
    for {
      projections <- Future.sequence(descriptors map { storage.projection(_) })
    } yield {
      EnumeratorP.mergeAll(projections.map(_.getColumnValues[X](path, selector)).toSeq: _*)
    }
  }

  def combine[X](enumerators: List[(JPath, EnumeratorP[X, SColumn, IO])]): EnumeratorP[X, SEvent, IO] = {
    def combine(enumerators: List[(JPath, EnumeratorP[X, SColumn, IO])]): EnumeratorP[X, SEvent, IO] = {
      enumerators match {
        case (selector, column) :: xs => 
          cogroupE[X, SEvent, SColumn, IO].apply(combine(xs), column).map {
            case Left3(sevent) => sevent
            case Middle3(((id, svalue), (_, cv))) => (id, svalue.set(selector, cv).getOrElse(sys.error("Cannot reassemble object: conflicting values for " + selector)))
            case Right3((id, cv)) => (id, SValue(selector, cv))
          }
        case Nil => EnumeratorP.empty[X, SEvent, IO]
      }
    }

    combine(enumerators)
  }
}
// vim: set ts=4 sw=4 et:
