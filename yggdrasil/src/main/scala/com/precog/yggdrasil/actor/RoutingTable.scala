package com.precog.yggdrasil
package actor

import table._
import util.CPathUtils

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.yggdrasil.nihdb._

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait RoutingTable extends Logging {

  private type Batch = (Long, Seq[IngestRecord])

  def batchMessages(events: Seq[(Long, EventMessage)]): Seq[ProjectionUpdate] = {
    val start = System.currentTimeMillis

    // the sequence of ProjectionUpdate objects to return
    val updates = ArrayBuffer.empty[ProjectionUpdate]

    // map used to aggregate IngestMessages by (Path, AccountId)
    val recordsByPath = mutable.Map.empty[(Path, Authorities), ArrayBuffer[Batch]]

    // process each message, aggregating ingest messages
    events.foreach {
      case (offset, IngestMessage(key, path, writeAs, data, jobid, timestamp)) =>
        val batches = recordsByPath.getOrElseUpdate((path, writeAs), ArrayBuffer.empty[Batch])
        batches += ((offset, data))

      case (_, ArchiveMessage(key, path, jobid, eventId, timestamp)) =>
        updates += ProjectionArchive(path, key, eventId)
    }

    // combine ingest messages by (path, owner), add to updates, then return
    recordsByPath.foreach {
      case ((path, writeAs), batches) =>
        updates += ProjectionInsert(path, batches, writeAs)
    }

    logger.debug("Batched %d events into %d updates in %d ms".format(events.size, updates.size, System.currentTimeMillis - start))

    updates
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
