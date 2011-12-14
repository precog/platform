package com.reportgrid.common

trait HashFunction extends (Array[Byte] => Array[Byte])

object Sha1HashFunction extends HashFunction {
  override def apply(bytes : Array[Byte]) : Array[Byte] = {
    val hash = java.security.MessageDigest.getInstance("SHA-1")
    hash.reset()
    hash.update(bytes)
    hash.digest()
  }
}

// vim: set ts=4 sw=4 et:
