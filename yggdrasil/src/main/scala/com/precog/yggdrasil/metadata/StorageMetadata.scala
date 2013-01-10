package com.precog.yggdrasil
package metadata 

import org.joda.time.DateTime

import com.precog.common.json._
import actor._

import com.precog.common._
import com.precog.common.security._

import blueeyes.bkka._
 
import akka.actor._
import akka.pattern.ask
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.dispatch.MessageDispatcher
import akka.util.Timeout
import akka.util.duration._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

trait StorageMetadata[M[+_]] { self =>
  implicit def M: Monad[M]

  def findChildren(path: Path): M[Set[Path]]
  def findSelectors(path: Path): M[Set[CPath]]
  def findProjections(path: Path, selector: CPath): M[Map[ProjectionDescriptor, ColumnMetadata]]
  def findPathMetadata(path: Path, selector: CPath): M[PathRoot]

  def findProjections(path: Path): M[Map[ProjectionDescriptor, ColumnMetadata]] = {
    findSelectors(path) flatMap { selectors => 
      selectors.traverse(findProjections(path, _)) map { proj =>
        if(proj.size == 0) {
          Map.empty[ProjectionDescriptor, ColumnMetadata]
        } else {
          proj.reduce(_ ++ _) 
        }
      }
    }
  }

  def findProjections(path: Path, selector: CPath, valueType: CType): M[Map[ProjectionDescriptor, ColumnMetadata]] = 
    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }

  def typeFilter(path: Path, selector: CPath, valueType: CType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType == valueType )
  }


  def liftM[T[_[+_], +_]](implicit T: Hoist[T]) = new StorageMetadata[({ type λ[+α] = T[M, α] })#λ] {
    private implicit val M0: Monad[M] = self.M
    val M: Monad[({ type λ[+α] = T[M, α] })#λ] = T(M0)

    def findChildren(path: Path) = self.findChildren(path).liftM[T]
    def findSelectors(path: Path) = self.findSelectors(path).liftM[T]
    def findProjections(path: Path, selector: CPath) = self.findProjections(path, selector).liftM[T]
    def findPathMetadata(path: Path, selector: CPath) = self.findPathMetadata(path, selector).liftM

    override def findProjections(path: Path) = self.findProjections(path).liftM

    override def findProjections(path: Path, selector: CPath, valueType: CType) = self.findProjections(path, selector, valueType).liftM

    override def typeFilter(path: Path, selector: CPath, valueType: CType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean =
      self.typeFilter(path, selector, valueType)(t)
  }
}

sealed trait PathMetadata 
case class PathRoot(children: Set[PathMetadata]) 

case class PathField(name: String, children: Set[PathMetadata]) extends PathMetadata
case class PathIndex(idx: Int, children: Set[PathMetadata]) extends PathMetadata
case class PathValue(valueType: CType, authorities: Authorities, descriptors: Map[ProjectionDescriptor, ColumnMetadata]) extends PathMetadata {
  def update(desc: ProjectionDescriptor, meta: ColumnMetadata) = 
    PathValue(valueType, authorities, descriptors + (desc -> meta))
}

class UserMetadataView[M[+_]](apiKey: APIKey, accessControl: AccessControl[M], metadata: StorageMetadata[M])(implicit val M: Monad[M]) extends StorageMetadata[M] {
  def findChildren(path: Path): M[Set[Path]] = {
    metadata.findChildren(path)
  }

  def findSelectors(path: Path): M[Set[CPath]] = {
    metadata.findSelectors(path) flatMap { selectors =>
      selectors traverse { selector =>
        findProjections(path, selector) map { result =>
          if(result.isEmpty) List.empty else List(selector)
        }
      } map { _.flatten }
    }
  }

  def findProjections(path: Path, selector: CPath): M[Map[ProjectionDescriptor, ColumnMetadata]] = {
    metadata.findProjections(path, selector) flatMap { pmap =>
      traverseFilter(pmap) {
        case (key, value) =>
          traverseForall(value) {
            case (colDesc, _) => 
              val ownerAccountIds = colDesc.authorities.ownerAccountIds
              accessControl.hasCapability(apiKey, Set(ReducePermission(path, ownerAccountIds)), some(new DateTime))
          }
      }
    }
  }
  
  def findPathMetadata(path: Path, selector: CPath): M[PathRoot] = {
    // TODO: This algorithm can be implemented in a single pass without all this nonsense.
    def restrictAccess(children: Set[PathMetadata]): M[Set[PathMetadata]] = {
      val mapped = children map {
        case PathField(name, children) => 
          restrictAccess(children).map(c => Some(PathField(name, c)))

        case PathIndex(index, children) => 
          restrictAccess(children).map(c => Some(PathIndex(index, c)))

        case p @ PathValue(_, authorities, _) =>
          (accessControl.hasCapability(apiKey, Set(ReducePermission(path, authorities.ownerAccountIds)), some(new DateTime)) map { _ option p })
      }

      mapped.sequence map { _.flatten }
    }

    def removeAllEmpty(children: Set[PathMetadata]): Set[PathMetadata] = {
       children.foldLeft(Set.empty[PathMetadata]){
         case (acc, PathField(name, children)) =>
           val fc = removeAllEmpty(children)
           if (!fc.isEmpty) { acc + PathField(name, fc) } else { acc }

         case (acc, PathIndex(index, children)) => 
           val fc = removeAllEmpty(children)
           if (!fc.isEmpty) { acc + PathIndex(index, fc) } else { acc }

         case (acc, p @ PathValue(_, _, _)) => acc + p
       }
    }

    metadata.findPathMetadata(path, selector).flatMap{ pr => 
      restrictAccess(pr.children) map removeAllEmpty map { PathRoot }
    }
  }

  def traverseFilter[A, B](as: Iterable[(A, B)])(f: ((A, B)) => M[Boolean]): M[Map[A, B]] = {
    for (tx <-  (as map { t => f(t) map { (t, _) } }).toStream.sequence) yield {
      tx collect { case (t, true) => t } toMap
    }
  }

  def traverseForall[A](as: Iterable[A])(f: A => M[Boolean]): M[Boolean] =
    as.map(f).toStream.sequence.map(_ forall identity)
}

class ActorStorageMetadata(actor: ActorRef, serviceTimeout0: Timeout)(implicit val asyncContext: ExecutionContext) extends StorageMetadata[Future] with Logging {
  implicit val M = AkkaTypeClasses.futureApplicative(asyncContext) 
  implicit val serviceTimeout = serviceTimeout0
 
  def findChildren(path: Path) = (actor ? FindChildren(path)).mapTo[Set[Path]] onFailure { 
    case e => logger.error("Error finding children for " + path, e) 
  }

  def findSelectors(path: Path) = (actor ? FindSelectors(path)).mapTo[Set[CPath]] onFailure { 
    case e => logger.error("Error finding selectors for " + path, e) 
  }

  def findProjections(path: Path, selector: CPath) = 
    (actor ? FindDescriptors(path, selector)).mapTo[Map[ProjectionDescriptor, ColumnMetadata]] onFailure { 
      case e => logger.error("Error finding projections for " + (path, selector), e) 
    }
  
  def findPathMetadata(path: Path, selector: CPath) = {
    logger.debug("Querying actor for path metadata")
    (actor ? FindPathMetadata(path, selector)).mapTo[PathRoot] onFailure { 
      case e => logger.error("Error finding pathmetadata for " + (path, selector), e) 
    }
  }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } onFailure { case e => logger.error("Error closing ActorStorageMetadata", e) }
}

