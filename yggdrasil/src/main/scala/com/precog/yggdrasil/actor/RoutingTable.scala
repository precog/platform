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
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.niflheim.NIHDB
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.vfs._

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

sealed trait BufferingState 

object BufferingState {
  case class Current(as: APIKey, writeAs: Authorities) extends BufferingState 
  case class Version(as: APIKey, writeAs: Authorities, streamId: UUID, canOverwrite: Boolean) extends BufferingState 
}

trait RoutingTable extends Logging {
  import NIHDB.Batch
  import BufferingState._

  // this method exists solely o fool scalac into allowing batchMessages to @tailrec
  private def batchMessages0(events: Stream[(Long, EventMessage)], buffers: mutable.Map[Path, (BufferingState, Option[JobId], ArrayBuffer[Batch])]) = batchMessages(events, buffers)
  @tailrec final def batchMessages(events: Stream[(Long, EventMessage)], buffers: mutable.Map[Path, (BufferingState, Option[JobId], ArrayBuffer[Batch])]): Stream[PathUpdateOp] = {
    events match {
      case (offset, IngestMessage(apiKey, path, writeAs, data, jobId, _, StreamRef.Append)) #:: xs =>
        val records = data.map(_.value)
        buffers.get(path) match {
          case Some((writeTo @ Current(`apiKey`, `writeAs`), `jobId`, buf)) =>
            batchMessages(xs, buffers += (path -> (writeTo, jobId, buf += Batch(offset, records))))

          case Some((writeTo @ Current(otherApiKey, otherWriteAs), otherJobId, buf)) =>
            Append(path, NIHDBData(buf), otherApiKey, otherWriteAs, otherJobId) #::
            batchMessages0(xs, buffers += (path -> (Current(apiKey, writeAs), jobId, ArrayBuffer(Batch(offset, records)))))

          case Some((Version(otherApiKey, otherWriteAs, otherStreamId, otherCanOverwrite), otherJobId, buf)) =>
            CreateNewVersion(path, NIHDBData(buf), otherStreamId, otherApiKey, otherWriteAs, otherCanOverwrite) #::
            batchMessages0(xs, buffers += (path -> (Current(apiKey, writeAs), jobId, ArrayBuffer(Batch(offset, records)))))

          case None =>
            batchMessages(xs, buffers += (path -> (Current(apiKey, writeAs), jobId, ArrayBuffer(Batch(offset, records)))))
        }

      case (offset, IngestMessage(apiKey, path, writeAs, data, jobId, _, StreamRef.NewVersion(streamId, terminal, canOverwrite))) #:: xs =>
        val records = data.map(_.value)
        buffers.get(path) match {
          case Some((writeTo @ Current(otherApiKey, otherWriteAs), otherJobId, buf)) =>
            // previously was appending to the current head at this path, so emit the accumulated append
            Append(path, NIHDBData(buf), otherApiKey, otherWriteAs, otherJobId) #::
            (if (terminal) {
              // this is a single-element create, so emit both the create and the makeCurrent
              CreateNewVersion(path, NIHDBData(List(Batch(offset, records))), streamId, apiKey, writeAs, canOverwrite) #::
              MakeCurrent(path, streamId, jobId) #::
              batchMessages0(xs, buffers -= path)
            } else {
              // accumulate buffered
              batchMessages0(xs, buffers += (path -> (Version(apiKey, writeAs, streamId, canOverwrite), jobId, ArrayBuffer(Batch(offset, records)))))
            })

          case Some((thisVersion @ Version(_, _, `streamId`, _), otherJobId, buf)) =>
            // appending to the same version, so just add to the buffer but don't fuss about the stream
            if (terminal) {
              CreateNewVersion(path, NIHDBData(buf :+ Batch(offset, records), streamId, apiKey, writeAs, canOverwrite) #::
              MakeCurrent(path, streamId, jobId) #::
              batchMessages0(xs, buffers -= path)
            } else {
              batchMessages(xs, buffers += (path -> (thisVersion, otherJobId, buf += Batch(offset, records))))
            }

          case Some((Version(otherApiKey, otherWriteAs, otherStreamId, otherCanOverwrite), otherJobId, buf)) =>
            // flush the existing buffers, then buffer the data from the current message (or emit if it's terminal)
            CreateNewVersion(path, NIHDBData(buf), otherStreamId, otherApiKey, otherWriteAs, otherCanOverwrite) #::
            (if (terminal) {
              CreateNewVersion(path, NIHDBData(List(Batch(offset, records))), streamId, apiKey, writeAs, canOverwrite) #::
              MakeCurrent(path, streamId, jobId) #::
              batchMessages0(xs, buffers -= path)
            } else {
              batchMessages0(xs, buffers += (path -> (Version(apiKey, writeAs, streamId, canOverwrite), jobId, ArrayBuffer(Batch(offset, records)))))
            })

          case None =>
            batchMessages(xs, buffers += (path -> (Version(apiKey, writeAs, streamId, canOverwrite), jobId, ArrayBuffer(Batch(offset, records)))))
        }

      case (offset, StoreFileMessage(apiKey, path, writeAs, jobId, eventId, fileContent, _, streamRef)) #:: xs =>
        

      case (offset, ArchiveMessage(_, path, jobId, _, _)) #:: xs =>
        ArchivePath(path, jobId) #:: batchMessages0(xs, buffers)
    }
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
