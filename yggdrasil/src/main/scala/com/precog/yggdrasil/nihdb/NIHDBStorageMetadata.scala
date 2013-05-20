/*
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

// FIXME: This is a bridge until everything can directly use SecureVFS

// FIXME: OK, this just really needs to go away, but unsure of trying to do it
// right now. All of the find* methods would have equivalents using VFS
// directly (and EitherT with a sprinkling of toOption or at least matching on
// MissingData) with not too much effort, but it's the use points of
// StorageMetadata I'm not sure of
class NIHDBStorageMetadata(apiKey: APIKey, vfs: SecureVFS[Future], storageTimeout: Timeout) extends StorageMetadata[Future] {
  implicit val timeout = storageTimeout

}

trait NIHDBStorageMetadataSource extends StorageMetadataSource[Future] {
  def projectionsActor: ActorRef
  def actorSystem: ActorSystem
  def storageTimeout: Timeout

  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = 
    new NIHDBStorageMetadata(apiKey, projectionsActor, actorSystem, storageTimeout)
}
*/
