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

trait StorageMetadata {

  implicit val dispatcher: MessageDispatcher

  def findSelectors(path: Path): Future[Seq[JPath]]

  def findProjections(path: Path): Future[Map[ProjectionDescriptor, ColumnMetadata]] = {
    findSelectors(path).flatMap { selectors => 
      Future.traverse( selectors )( findProjections(path, _) ) map { _.reduce(_ ++ _) }
    }
  }

  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]]

  def findProjections(path: Path, selector: JPath, valueType: SType): Future[Map[ProjectionDescriptor, ColumnMetadata]] = 
    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }

  def typeFilter(path: Path, selector: JPath, valueType: SType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType =~ valueType )
  }
}

trait MetadataView extends StorageMetadata

class IdentityMetadataView(metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView {
  def findSelectors(path: Path) = metadata.findSelectors(path)
  def findProjections(path: Path, selector: JPath) = metadata.findProjections(path, selector)
}

class UserMetadataView(uid: String, accessControl: AccessControl, metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView { 
  
  def findSelectors(path: Path) = {
    metadata.findSelectors(path) flatMap { selectors =>
      Future.traverse(selectors) { selector =>
        findProjections(path, selector) map { result =>
          if(result.isEmpty) List.empty else List(selector)
        }
      } map { _.flatten }
    }
  }

  def findProjections(path: Path, selector: JPath) = {
    metadata.findProjections(path, selector) map { _.filter {
        case (key, value) =>
          value forall { 
            case (colDesc, _) => 
              val uids = colDesc.authorities.uids
              accessControl.mayAccessData(uid, path, uids, DataQuery)
          }
      }
    }
  }
}

class ShardMetadata(actor: ActorRef)(implicit val dispatcher: MessageDispatcher) extends StorageMetadata {

  implicit val serviceTimeout: Timeout = 10 seconds
 
  def findSelectors(path: Path) = actor ? FindSelectors(path) map { _.asInstanceOf[Seq[JPath]] }

  def findProjections(path: Path, selector: JPath) = 
    actor ? FindDescriptors(path, selector) map { _.asInstanceOf[Map[ProjectionDescriptor, ColumnMetadata]] }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

}

