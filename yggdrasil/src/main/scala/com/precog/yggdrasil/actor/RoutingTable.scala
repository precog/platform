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
    val recordsByPath = mutable.Map.empty[(Path, Authorities), ArrayBuffer[Batch]]

    // process each message, aggregating ingest messages
    events.foreach {
      case (offset, IngestMessage(key, path, writeAs, data, jobid, timestamp)) =>
        val batches = recordsByPath.getOrElseUpdate((path, writeAs), ArrayBuffer.empty[Batch])
        batches += ((offset, data.map(_.value)))

      case (_, ArchiveMessage(key, path, jobid, eventId, timestamp)) =>
        val uuid = UUID.randomUUID
        updates += Create(path, NIHDBData.Empty, uuid, None, false)
        updates += Replace(path, uuid)
    }

    // combine ingest messages by (path, owner), add to updates, then return
    recordsByPath.foreach {
      case ((path, writeAs), batches) =>
        updates += Append(path, NIHDBData(batches.toSeq), None, writeAs)
    }

    logger.debug("Batched %d events into %d updates in %d ms".format(events.size, updates.size, System.currentTimeMillis - start))

    updates
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
