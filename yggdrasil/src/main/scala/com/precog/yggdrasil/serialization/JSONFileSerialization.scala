package com.precog
package yggdrasil
package serialization

import com.precog.common.VectorCase

import blueeyes.json._
import blueeyes.json.JsonDSL._

import java.io._
import scala.annotation.tailrec
import scalaz.iteratee._
import scalaz.effect._

trait JSONSEventChunkSerialization extends FileSerialization[Vector[SEvent]] {
  private def writeEvent(out: DataOutputStream, ids: Identities, sv: SValue) = {
    out.writeInt(ids.size)
    for (id <- ids) out.writeLong(id)
    val jvStr = compact(render(sv.toJValue))
    out.writeUTF(jvStr)
  }

  def writeElement(out: DataOutputStream, v: Vector[SEvent]): IO[Unit] = IO { 
    out.writeInt(v.size)
    for ((ids, sv) <- v) writeEvent(out, ids, sv)
  }

  private def readEvent(in: DataInputStream): SEvent = {
    val idCount = in.readInt()
    val ids = VectorCase.fromSeq((0 until idCount).map(_ => in.readLong))
    val jstr = in.readUTF
    (ids, SValue.fromJValue(JsonParser.parse(jstr)))
  }

  def readElement(in: DataInputStream): IO[Option[Vector[SEvent]]] = IO {
    @tailrec
    def readChunk(acc: Vector[SEvent], remaining: Int): Vector[SEvent] = {
      if (remaining > 0) readChunk(acc :+ readEvent(in), remaining - 1)
      else               acc
    }

    try {
      val slurped = readChunk(Vector(), in.readInt())
      if (slurped.size == 0) None else Some(slurped)
    } catch {
      case ex: EOFException => None
    }
  }
}
// vim: set ts=4 sw=4 et:
