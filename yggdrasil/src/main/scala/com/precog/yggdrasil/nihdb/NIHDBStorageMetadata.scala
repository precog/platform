/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
