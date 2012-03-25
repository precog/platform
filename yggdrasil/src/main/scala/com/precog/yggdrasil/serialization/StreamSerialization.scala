package com.precog.yggdrasil.serialization

import java.io._
import java.util.zip._

trait StreamSerialization {
  def iStream(file: File): DataInputStream
  def oStream(file: File): DataOutputStream
}

trait ZippedStreamSerialization extends StreamSerialization {
  def iStream(file: File) = new DataInputStream(new GZIPInputStream(new FileInputStream(file)))
  def oStream(file: File) = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(file)))
}


// vim: set ts=4 sw=4 et:
