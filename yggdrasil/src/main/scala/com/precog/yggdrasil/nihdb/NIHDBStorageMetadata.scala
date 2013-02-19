package com.precog.yggdrasil
package nihdb

import com.precog.common.{CType, Path}
import com.precog.common.json.CPath
import com.precog.common.security.APIKey
import com.precog.yggdrasil.metadata.{PathStructure, StorageMetadata}
import com.precog.yggdrasil.table.ColumnRef

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Future, Promise}
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.bkka.FutureMonad

import scalaz.Monad

class NIHDBStorageMetadata(apiKey: APIKey, projectionsActor: ActorRef, actorSystem: ActorSystem, storageTimeout: Timeout) extends StorageMetadata[Future] {
  implicit val asyncContext = actorSystem.dispatcher
  implicit val timeout = storageTimeout

  implicit def M: Monad[Future] = new FutureMonad(actorSystem.dispatcher)

  def findDirectChildren(path: Path): Future[Set[Path]] = {
    (projectionsActor ? FindChildren(path)).mapTo[Set[Path]]
  }

  def findSize(path: Path): Future[Long] =
    (projectionsActor ? AccessProjection(path, apiKey)).mapTo[Option[NIHDBProjection]].flatMap {
      case Some(proj) => proj.length
      case None => Promise.successful(0L)(asyncContext)
    }

  def findSelectors(path: Path): Future[Set[CPath]] =
    (projectionsActor ? AccessProjection(path, apiKey)).mapTo[Option[NIHDBProjection]].flatMap {
      case Some(proj) => proj.structure.map(_.map(_.selector))
      case None => Promise.successful(Set.empty[CPath])(asyncContext)
    }

  def findStructure(path: Path, selector: CPath): Future[PathStructure] =
    (projectionsActor ? AccessProjection(path, apiKey)).mapTo[Option[NIHDBProjection]].flatMap {
      case Some(proj) => 
        for {
          structure <- proj.structure
        } yield {
          val types : Map[CType, Long] = structure.collect { 
            // FIXME: This should use real counts once we have that stored in NIHDB blocks
            case ColumnRef(selector, ctype) if selector.hasPrefix(selector) => (ctype, 0L)
          }.groupBy(_._1).map { case (tpe, values) => (tpe, values.map(_._2).sum) }

          PathStructure(types, structure.map(_.selector))
        }

      case None => Promise.successful(PathStructure.Empty)(asyncContext)
    }
}

trait NIHDBStorageMetadataSource extends StorageMetadataSource[Future] {
  val projectionsActor: ActorRef
  val actorSystem: ActorSystem
  val storageTimeout: Timeout

  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = 
    new NIHDBStorageMetadata(apiKey, projectionsActor, actorSystem, storageTimeout)
}

