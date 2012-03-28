package com.precog.util

trait Close[A] {
  def close(a: A): Unit
}

object Close {
  import java.io._
  implicit object DataInputStreamClose extends Close[DataInputStream] {
    def close(a: DataInputStream) = {
      a.close
    }
  }

  implicit object DataOutputStreamClose extends Close[DataOutputStream] {
    def close(a: DataOutputStream) = {
      a.flush
      a.close
    }
  }
}


// vim: set ts=4 sw=4 et:
