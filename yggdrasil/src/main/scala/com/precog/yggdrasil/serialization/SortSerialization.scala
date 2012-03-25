package com.precog
package yggdrasil
package serialization

import java.io._

trait SortSerialization[E] extends StreamSerialization {
  // Write out from the buffer, indices [0,limit)
  def write(out: DataOutputStream, values: Array[E], limit: Int): Unit
  def reader(in: DataInputStream): Iterator[E]
}


// vim: set ts=4 sw=4 et:
