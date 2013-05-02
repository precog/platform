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

import table._
import util.CPathUtils

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.vfs._

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait RoutingTable extends Logging {

  private type Batch = (Long, Seq[JValue])

  // DREAMON: Some day, make this a stream transformer
  def batchMessages(events: Seq[(Long, EventMessage)]): Seq[PathUpdateOp] = {
    sys.error("todo") /*
    val start = System.currentTimeMillis

    // the sequence of ProjectionUpdate objects to return
    val updates = ArrayBuffer.empty[PathUpdateOp]

    // map used to aggregate IngestMessages by (Path, AccountId)
    val recordsByPath = mutable.Map.empty[Path, (WriteTo, ArrayBuffer[Batch])]

    // process each message, aggregating ingest messages
    events.foreach {
      case (offset, IngestMessage(apiKey, path, writeAs, data, jobId, _, streamId, mode @ (StoreMode.Create | StoreMode.Replace))) => 
        // Send along whatever we've batched up to this point, then start a new
        // update. This could get messy if the create fails due to not
        // overwriting existing data, but more ingests are coming along
        // expecting to be a part of this. We probably want to require that
        // "CreateMode" ingests provide a stream ID
        val version = streamId getOrElse {
          UUID.randomUUID
        }

        recordsByPath.get(path) foreach {
          case (k0, a0, buffer) if buffer.length > 0 =>
            updates += Append(path, NIHDBData(buffer), WriteTo.Current(k0, a0), jobId)
            recordsByPath -= path
        }

        updates += CreateNewVersion(path, NIHDBData(data), version, apiKey, writeAs, mode == StoreMode.Replace)

      case (offset, IngestMessage(apiKey, path, writeAs, data, jobId, _, streamId, StoreMode.Append)) => 
        // find whether there's an existing stream that we should be appending to
        // depending upon store mode, we may need creates and replaces put into the stream of updates
        recordsByPath.get(path) match {
          case Some((`apiKey`, `writeAs`, buffer)) =>
            buffer += (offset -> data.map(_.value))

          case Some((k0, a0, buffer)) =>
            updates += Append(path, NIHDBData(buffer), WriteTo.Current(k0, a0), jobId)
            recordsByPath += (path -> (apiKey, writeAs, ArrayBuffer[Batch]((offset, data.map(_.value)))))

          case None =>
            recordsByPath += (path -> (apiKey, writeAs, ArrayBuffer[Batch]((offset, data.map(_.value)))))
        }

      // Should StoreFileMessage carry a store mode?
      case (offset, StoreFileMessage(apiKey, path, writeAs, jobId, eventId, fileContent, timestamp, streamId)) =>
        val version = streamId.getOrElse(UUID.randomUUID)
        updates += CreateNewVersion(path, BlobData(fileContent.data, fileContent.mimeType), version, apiKey, writeAs, false)
        updates += MakeCurrent(path, version)

      case (_, ArchiveMessage(key, path, jobId, eventId, timestamp)) =>
        updates += ArchivePath(path, jobId)
    }

    // Flush any remaining records by path here

    logger.debug("Batched %d events into %d updates in %d ms".format(events.size, updates.size, System.currentTimeMillis - start))
    updates
    */
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
