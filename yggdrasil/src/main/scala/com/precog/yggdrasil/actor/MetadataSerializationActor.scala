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
package com.precog.yggdrasil
package actor

import metadata._
import leveldb._

import com.precog.util._
import com.precog.common._

import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import akka.actor.Actor
import akka.dispatch.Future

import scalaz._
import scalaz.effect._
import scalaz.std.list._
import scalaz.syntax.apply._
import scalaz.syntax.traverse._

import com.weiglewilczek.slf4s.Logging

import java.io.File

case class SaveMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], checkpoint: Option[YggCheckpoint]) 

class MetadataSerializationActor(shardId: String, storage: MetadataStorage, systemCoordination: SystemCoordination) extends Actor with Logging {
  def receive = {
    // TODO: Does it make any sense to save metadata *without* a checkpoint?
    case SaveMetadata(metadata, Some(checkpoint)) => 
      val io: List[IO[Validation[Throwable, Unit]]] = 
        metadata.map({ case (desc, meta) => storage.updateMetadata(desc, MetadataRecord(meta, checkpoint.messageClock)) })(collection.breakOut)

      // if some metadata fails to be written and we consequently don't write the checkpoint,
      // then the restore process for each projection will need to skip all message ids prior
      // to the checkpoint clock associated with that metadata
      val errors = (io.sequence[IO, Validation[Throwable, Unit]] map { _.collect { case Failure(t) => t } } unsafePerformIO)
      if (errors.isEmpty) {
        systemCoordination.saveYggCheckpoint(shardId, checkpoint)
      }
  }
}

