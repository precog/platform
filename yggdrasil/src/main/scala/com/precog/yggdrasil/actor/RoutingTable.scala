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

  def batchMessages(events: Seq[(Long, EventMessage)]): Seq[PathUpdateOp] = {
    val start = System.currentTimeMillis

    // the sequence of ProjectionUpdate objects to return
    val updates = ArrayBuffer.empty[PathUpdateOp]

    // map used to aggregate IngestMessages by (Path, AccountId)
    val recordsByPath = mutable.Map.empty[Path, (APIKey, Authorities, ArrayBuffer[Batch])]

    // process each message, aggregating ingest messages
    events.foreach {
      case (offset, IngestMessage(apiKey, path, writeAs, data, jobId, _, streamId)) =>
        recordsByPath.get(path) match {
          case Some((`apiKey`, `writeAs`, buffer)) =>
            recordsByPath += (path -> (apiKey, writeAs, buffer + ((offset, data.map(_.value)))))

          case Some((k0, a0, buffer)) =>
            updates += Append(path, NIHDBData(buffer), WriteTo.Current(k0, a0), jobId)
            recordsByPath += (path -> (apiKey, writeAs, ArrayBuffer[Batch]((offset, data.map(_.value)))))

          case None => 
            recordsByPath += (path -> (apiKey, writeAs, ArrayBuffer[Batch]((offset, data.map(_.value)))))
        }

      case (offset, StoreFileMessage(apiKey, path, writeAs, jobId, eventId, fileContent, timestamp, streamId) =>
        updates += CreateNewVersion(path, BlobData(sfm.content.data, sfm.content.mimeType), sfm.streamId.getOrElse(UUID.randomUUID), apiKey, writeAs, false)

      case (_, ArchiveMessage(key, path, jobid, eventId, timestamp)) =>
        updates += ArchivePath(path, jobId)
    }

    // combine ingest messages by (path, owner), add to updates, then return
    recordsByPath.foreach {
      case ((path, writeAs), batches) =>
    }

    logger.debug("Batched %d events into %d updates in %d ms".format(events.size, updates.size, System.currentTimeMillis - start))

    updates
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
