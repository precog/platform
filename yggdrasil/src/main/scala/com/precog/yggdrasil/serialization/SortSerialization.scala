package com.precog.yggdrasil
package serialization

import java.io._
import scala.annotation.tailrec

trait SortSerialization[A] extends StreamSerialization with RunlengthFormatting[A] {
  // Write out from the buffer, indices [0,limit)
  def write(out: DataOutputStream, values: Array[A], limit: Int): Unit = {
    @tailrec def write(i: Int, header: Header): Unit = {
      if (i < limit) {
        val sv = values(i)
        val newHeader = headerFor(sv)
        if (header == newHeader) {
          out.writeInt(ValueFlag)
          writeRecord(out, sv, header)
          write(i + 1, header)
        } else {
          out.writeInt(HeaderFlag)
          writeHeader(out, newHeader)
          out.writeInt(ValueFlag)
          writeRecord(out, sv, newHeader)
          write(i + 1, newHeader)
        }
      } 
    }
    
    out.writeInt(limit)
    write(0, null.asInstanceOf[Header])
  }

  def reader(in: DataInputStream): Iterator[A] = {
    new Iterator[A] {
      private var remaining: Int = in.readInt()
      private var header: Header = null.asInstanceOf[Header]

      private var _next = precomputeNext()

      def hasNext = _next != null
      def next: A = {
        assert (_next != null)
        val tmp = _next
        _next = precomputeNext()
        tmp
      }

      @tailrec private def precomputeNext(): A = {
        if (remaining > 0) {
          in.readInt() match {
            case HeaderFlag =>
              header = readHeader(in)
              precomputeNext()

            case ValueFlag => 
              remaining -= 1
              assert (header != null)
              readRecord(in, header)
          }
        } else {
          null.asInstanceOf[A]
        }
      }
    }
  }
}
