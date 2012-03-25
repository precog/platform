package com.precog.yggdrasil.serialization

import java.io._

trait IncrementalSerialization[A] {
  trait IncrementalWriter {
    def write(out: DataOutputStream, a: A): IncrementalWriter
  }

  trait IncrementalReader {
    def read(in: DataInputStream): (A, IncrementalReader)
  }

  def writer: IncrementalWriter
  def reader: IncrementalReader
}


// vim: set ts=4 sw=4 et:
