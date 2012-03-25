package com.precog
package yggdrasil
package serialization

import com.precog.common.VectorCase

import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.ColumnType._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._

trait SValueSortSerialization extends SortSerialization[SValue] with BinarySValueSerialization {
  case class Header(structure: Seq[(JPath, ColumnType)])
  final val HeaderFlag = 0
  final val ValueFlag = 1

  def write(out: DataOutputStream, values: Array[SValue]): Unit = {
    @tailrec def write(i: Int, header: Option[Header]): Unit = {
      if (i < values.length) {
        val sv = values(i)
        val newHeader = Header(sv.structure)
        if (header.exists(_ == newHeader)) {
          writeRecord(out, sv)
          write(i + 1, header)
        } else {
          writeHeader(out, newHeader)
          writeRecord(out, sv)
          write(i + 1, Some(newHeader))
        }
      }
    }
    
    out.writeInt(values.length)
    write(0, None)
  }

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    out.writeInt(HeaderFlag)
    writeStructure(out, header.structure)
  }

  def writeRecord(out: DataOutputStream, sv: SValue): Unit = {
    out.writeInt(ValueFlag)
    writeValue(out, sv)
  }

  def reader(in: DataInputStream): Iterator[SValue] = {
    new Iterator[SValue] {
      private val remaining: Int = in.readInt()
      private var header: Header = null.asInstanceOf[Header]

      private var _next = precomputeNext()

      def hasNext = _next != null
      def next: SValue = {
        assert (_next != null)
        val tmp = _next
        _next = precomputeNext()
        tmp
      }

      @tailrec private def precomputeNext(): SValue = {
        if (remaining > 0) {
          in.readInt() match {
            case HeaderFlag =>
              header = readHeader(in)
              precomputeNext()

            case ValueFlag => 
              assert (header != null)
              readValue(in, header.structure)
          }
        } else {
          null.asInstanceOf[SValue]
        }
      }
    }
  }

  def readHeader(in: DataInputStream): Header = Header(readStructure(in))
}
// vim: set ts=4 sw=4 et:
