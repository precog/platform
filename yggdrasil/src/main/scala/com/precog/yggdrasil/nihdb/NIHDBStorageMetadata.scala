package com.precog.yggdrasil
package nihdb

import com.precog.common.Path
import com.precog.common.json.CPath
import com.precog.common.security.APIKey
import com.precog.yggdrasil.metadata.StorageMetadata

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

  def findChildren(path: Path): Future[Set[Path]] = {
    (projectionsActor ? FindChildren(path)).mapTo[Set[Path]]
  }

  def findSelectors(path: Path): Future[Set[CPath]] =
    (projectionsActor ? AccessProjection(path, apiKey)).mapTo[Option[NIHDBProjection]].flatMap {
      case Some(proj) => proj.structure.map(_.map(_.selector))
      case None => Promise.successful(Set.empty[CPath])(asyncContext)
    }
}

trait NIHDBStorageMetadataSource extends StorageMetadataSource[Future] {
  val projectionsActor: ActorRef
  val actorSystem: ActorSystem
  val storageTimeout: Timeout

  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = 
    new NIHDBStorageMetadata(apiKey, projectionsActor, actorSystem, storageTimeout)
}

