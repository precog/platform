package com.precog.yggdrasil.serialization

import java.io._
import scala.annotation.tailrec

trait IncrementalSerialization[A] extends StreamSerialization with RunlengthFormatting[A] {
  final val EOFFlag = Int.MinValue + 2

  final class IncrementalWriter(header: Header) {
    def write(out: DataOutputStream, value: A): IncrementalWriter = {
      val newHeader = headerFor(value)
      if (header == newHeader) {
        out.writeInt(ValueFlag)
        writeRecord(out, value)
        this
      } else {
        out.writeInt(HeaderFlag)
        writeHeader(out, newHeader)
        out.writeInt(ValueFlag)
        writeRecord(out, value)
        new IncrementalWriter(newHeader)
      }
    }

    def finish(out: DataOutputStream): Unit = {
      out.writeInt(EOFFlag)
    }
  }

  final class IncrementalReader {
    def read(in: DataInputStream, closeOnEOF: Boolean): Iterator[A] = {
      new Iterator[A] {
        private var header: Header = null.asInstanceOf[Header]
        private var _next = precomputeNext()

        def hasNext = _next != null
        def next: A = {
          if (_next == null) throw new IllegalStateException("next called on iterator beyond end of input")
          val tmp = _next
          _next = precomputeNext()
          tmp
        }

        @tailrec private def precomputeNext(): A = {
          in.readInt() match {
            case HeaderFlag =>
              header = readHeader(in)
              precomputeNext()

            case ValueFlag => 
              assert (header != null)
              readRecord(in, header)

            case EOFFlag =>
              if (closeOnEOF) in.close()
              null.asInstanceOf[A]
          }
        }
      }
    }
  }

  def writer: IncrementalWriter = new IncrementalWriter(null.asInstanceOf[Header])
  def reader: IncrementalReader = new IncrementalReader
}


// vim: set ts=4 sw=4 et:
