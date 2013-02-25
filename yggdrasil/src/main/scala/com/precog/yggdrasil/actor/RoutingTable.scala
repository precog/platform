package com.precog.yggdrasil
package actor

import table._
import util.CPathUtils

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.json._
import com.precog.yggdrasil.nihdb._

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait RoutingTable extends Logging {
  def batchMessages(events: Seq[EventMessage]): Seq[ProjectionUpdate] = {
    val start = System.currentTimeMillis

    // the sequence of ProjectionUpdate objects to return
    val updates = ArrayBuffer.empty[ProjectionUpdate]

    // map used to aggregate IngestMessages by (Path, AccountId)
    val recordsByPath = mutable.Map.empty[(Path, AccountId), ArrayBuffer[IngestRecord]]

    // process each message, aggregating ingest messages
    events.foreach {
      case IngestMessage(key, path, owner, data, jobid) =>
        val buf = recordsByPath.getOrElseUpdate((path, owner), ArrayBuffer.empty[IngestRecord])
        buf ++= data

      case msg: ArchiveMessage =>
        updates += ProjectionArchive(msg.archive.path, msg.eventId)
    }

    // combine ingest messages by (path, owner), add to updates, then return
    recordsByPath.foreach {
      case ((path, owner), values) =>
        updates += ProjectionInsert(path, values, owner)
    }

    logger.debug("Batched %d events into %d updates in %d ms".format(events.size, updates.size, System.currentTimeMillis - start))

    updates
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
