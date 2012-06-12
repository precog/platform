package com.precog.yggdrasil
package metadata 

import actor._

import com.precog.common._
import com.precog.common.security._

import blueeyes.json.JPath
 
import akka.actor._
import akka.pattern.ask
import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout
import akka.util.duration._

import com.weiglewilczek.slf4s.Logging

trait StorageMetadata {

  implicit val dispatcher: MessageDispatcher

  def findChildren(path: Path): Future[Set[Path]]
  def findSelectors(path: Path): Future[Seq[JPath]]
  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]]
  def findPathMetadata(path: Path, selector: JPath): Future[PathRoot]

  def findProjections(path: Path): Future[Map[ProjectionDescriptor, ColumnMetadata]] = {
    findSelectors(path).flatMap { selectors => 
      Future.traverse( selectors )( findProjections(path, _) ) map { proj =>
        if(proj.size == 0) {
          Map.empty[ProjectionDescriptor, ColumnMetadata]
        } else {
          proj.reduce(_ ++ _) }
        }
    }
  }

  def findProjections(path: Path, selector: JPath, valueType: SType): Future[Map[ProjectionDescriptor, ColumnMetadata]] = 
    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }

  def typeFilter(path: Path, selector: JPath, valueType: SType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType =~ valueType )
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

trait MetadataView extends StorageMetadata

class IdentityMetadataView(metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView {
  def findChildren(path: Path) = metadata.findChildren(path)
  def findSelectors(path: Path) = metadata.findSelectors(path)
  def findProjections(path: Path, selector: JPath) = metadata.findProjections(path, selector)
  def findPathMetadata(path: Path, selector: JPath) = metadata.findPathMetadata(path, selector)
}

class UserMetadataView(uid: String, accessControl: AccessControl, metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView { 
 
  def findChildren(path: Path): Future[Set[Path]] = {
    metadata.findChildren(path) flatMap { paths =>
      Future.traverse(paths) { p =>
        val tPath = path / p
        accessControl.mayAccessPath(uid, tPath, PathRead) map {
          case true => Set(p)
          case false => Set.empty
        }
      }.map{ _.flatten }
    }
  }

  def findSelectors(path: Path): Future[Seq[JPath]] = {
    metadata.findSelectors(path) flatMap { selectors =>
      Future.traverse(selectors) { selector =>
        findProjections(path, selector) map { result =>
          if(result.isEmpty) List.empty else List(selector)
        }
      } map { _.flatten }
    }
  }

  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]] = {
    metadata.findProjections(path, selector) flatMap { pmap =>
      traverseFilter(pmap) {
        case (key, value) =>
          traverseForall(value) {
            case (colDesc, _) => 
              val uids = colDesc.authorities.uids
              accessControl.mayAccessData(uid, path, uids, DataQuery)
          }
      }
    }
  }
  
  def findPathMetadata(path: Path, selector: JPath): Future[PathRoot] = {
    // TODO: This algorithm can be implemented in a single pass without all this nonsense.
    def restrictAccess(children: Set[PathMetadata]): Future[Set[PathMetadata]] = {
      val mapped = children.foldLeft(Set.empty[Future[Option[PathMetadata]]]) {
        case (acc, PathField(name, children)) => 
          acc + restrictAccess(children).map(c => Some(PathField(name, c)))

        case (acc, PathIndex(index, children)) => 
          acc + restrictAccess(children).map(c => Some(PathIndex(index, c)))

        case (acc, p @ PathValue(_, authorities, _)) =>
          acc + accessControl.mayAccessData(uid, path, authorities.uids, DataQuery) map { _ option p }
      }

      Future.fold(mapped)(Set.empty[PathMetadata]) {
        case (acc, pm) => acc ++ pm
      }
    }

    def filter2(children: Set[PathMetadata]): Set[PathMetadata] = {
       children.foldLeft(Set.empty[PathMetadata]){
         case (acc, PathField(name, children)) =>
           val fc = filter2(children)
           if(fc.size > 0) { acc + PathField(name, fc) } else { acc }
         case (acc, PathIndex(index, children)) => 
           val fc = filter2(children)
           if(fc.size > 0) { acc + PathIndex(index, fc) } else { acc }
         case (acc, p @ PathValue(_, _, _)) => acc + p
       }
    }

    accessControl.mayAccessPath(uid, path, PathRead).flatMap {
      case true =>
        metadata.findPathMetadata(path, selector).flatMap{ pr => 
          restrictAccess(pr.children).map{ filter2 }.map{ PathRoot }
        }
      case false =>
        Future(PathRoot(Set.empty))
    }
  }

  def traverseFilter[A, B](as: Traversable[(A, B)])(f: ((A, B)) => Future[Boolean]): Future[Map[A, B]] = {
    val tx = as.map( t => f(t) map { (t, _) } )
    Future.fold(tx)(Map.empty[A,B]){
      case (acc, (t, b)) => if(b) { acc + t } else { acc }
    }
  }

  def traverseForall[A](as: Traversable[A])(f: A => Future[Boolean]): Future[Boolean] =
    if(as.size == 0) { Future(true) } else { Future.reduce(as.map(f))(_ && _) }
}

class ActorStorageMetadata(actor: ActorRef)(implicit val dispatcher: MessageDispatcher) extends StorageMetadata with Logging {
  logger.debug("ActorStorageMetadata init. Sends to " + actor + " via " + dispatcher)

  implicit val serviceTimeout: Timeout = 10 seconds
 
  def findChildren(path: Path) = actor ? FindChildren(path) map { _.asInstanceOf[Set[Path]] } onFailure { case e => logger.error("Error finding children for " + path, e) }

  def findSelectors(path: Path) = actor ? FindSelectors(path) map { _.asInstanceOf[Seq[JPath]] } onFailure { case e => logger.error("Error finding selectors for " + path, e) }

  def findProjections(path: Path, selector: JPath) = 
    actor ? FindDescriptors(path, selector) map { _.asInstanceOf[Map[ProjectionDescriptor, ColumnMetadata]] } onFailure { case e => logger.error("Error finding projections for " + (path, selector), e) }
  
  def findPathMetadata(path: Path, selector: JPath) = {
    logger.debug("Querying actor for path metadata")
    actor ? FindPathMetadata(path, selector) map { _.asInstanceOf[PathRoot] } onFailure { case e => logger.error("Error finding pathmetadata for " + (path, selector), e) }
  }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } onFailure { case e => logger.error("Error closing ActorStorageMetadata", e) }

}
