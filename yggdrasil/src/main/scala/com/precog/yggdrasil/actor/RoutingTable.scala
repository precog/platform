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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait RoutingTable {
  def batchMessages(events: Seq[EventMessage]): Seq[ProjectionUpdate] = {
    // the sequence of ProjectionUpdate objects to return
    val updates = ArrayBuffer.empty[ProjectionUpdate]

    // map used to aggregate IngestMessages by Path
    val recordsByPath = mutable.Map.empty[Path, (Path, AccountId, ArrayBuffer[IngestRecord])]

    // process each message, aggregating ingest messages
    events.foreach {
      case IngestMessage(key, path, owner, data, jobid) =>
        val tpl = recordsByPath.getOrElseUpdate(path, (path, owner, ArrayBuffer.empty[IngestRecord]))
        tpl._3 ++= data

      case msg: ArchiveMessage =>
        updates += ProjectionArchive(msg.archive.path, msg.eventId)
    }

    // combine ingest messages by path and add to updates, then return
    recordsByPath.values.foreach {
      case (path, owner, values) =>
        updates += ProjectionInsert(path, values, owner)
    }
    updates
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable
