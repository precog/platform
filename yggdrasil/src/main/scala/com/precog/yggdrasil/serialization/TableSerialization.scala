package com.precog.yggdrasil
package serialization

import java.io.DataOutputStream
import java.io.DataInputStream

trait TableSerialization {
  def write(out: DataOutputStream, slice: Slice): Unit
  def read(in: DataInputStream): Slice
}

// vim: set ts=4 sw=4 et:
