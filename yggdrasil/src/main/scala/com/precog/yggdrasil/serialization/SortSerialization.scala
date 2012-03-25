package com.precog
package yggdrasil
package serialization

import java.io._

trait SortSerialization[E <: AnyRef] extends StreamSerialization {
  def write(out: DataOutputStream, values: Array[E]): Unit
  def reader(in: DataInputStream): Iterator[E]
}


// vim: set ts=4 sw=4 et:
