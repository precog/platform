package com.precog.yggdrasil
package nihdb

import com.precog.common._
import com.precog.common.security._
import com.precog.yggdrasil.metadata.{PathStructure, StorageMetadata}
import com.precog.yggdrasil.vfs._

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Future, Promise}
import akka.pattern.ask
import akka.util.Timeout

import com.precog.niflheim._

import blueeyes.bkka.FutureMonad

import scalaz._
import scalaz.syntax.monad._

class NIHDBStorageMetadata(apiKey: APIKey, projectionsActor: ActorRef, actorSystem: ActorSystem, storageTimeout: Timeout) extends StorageMetadata[Future] {
  implicit val asyncContext = actorSystem.dispatcher
  implicit val timeout = storageTimeout

  implicit def M: Monad[Future] = new FutureMonad(actorSystem.dispatcher)

  def findDirectChildren(path: Path): Future[Set[Path]] = {
    val paths = (projectionsActor ? FindChildren(path, apiKey)).mapTo[Set[Path]]
    paths.map(_.flatMap(_ - path))
  }

  private def findProjection(path: Path): Future[Option[NIHDBProjection]] =
    (projectionsActor ? ReadProjection(path, Version.Current, Some(apiKey))).mapTo[ReadProjectionResult].map(_.projection)

  def findSize(path: Path): Future[Long] = findProjection(path).map { _.map(_.length).getOrElse(0L) }

  def findSelectors(path: Path): Future[Set[CPath]] = findProjection(path).flatMap {
    case Some(proj) => proj.structure.map(_.map(_.selector))
    case None => Promise.successful(Set.empty[CPath])(asyncContext)
  }

  def findStructure(path: Path, selector: CPath): Future[PathStructure] = {
    OptionT(findProjection(path)) flatMapF { projection =>
      for {
        children <- projection.structure
      } yield {
        PathStructure(projection.reduce(Reductions.count, selector), children.map(_.selector))
      }
    } getOrElse PathStructure.Empty
  }

  def currentAuthorities(path: Path): Future[Option[Authorities]] = {
    findProjection(path) map { _ map { _.authorities } }
  }

  def currentVersion(path: Path) = {
    (projectionsActor ? CurrentVersion(path, apiKey)).mapTo[Option[VersionEntry]]
  }
}

trait NIHDBStorageMetadataSource extends StorageMetadataSource[Future] {
  val projectionsActor: ActorRef
  val actorSystem: ActorSystem
  val storageTimeout: Timeout

  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = 
    new NIHDBStorageMetadata(apiKey, projectionsActor, actorSystem, storageTimeout)
}
