package com.precog.util

trait Close[-A] {
  def close(a: A): Unit
}

object Close {
  import java.io._

  implicit object InputStreamClose extends Close[InputStream] {
    def close(in: InputStream) = {
      in.close
    }
  }

  implicit object OutputStreamClose extends Close[OutputStream] {
    def close(a: OutputStream) = {
      a.flush
      a.close
    }
  }
}


// vim: set ts=4 sw=4 et:
