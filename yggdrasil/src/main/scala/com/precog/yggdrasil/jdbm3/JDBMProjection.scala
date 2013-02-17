package com.precog.yggdrasil
package jdbm3

object JDBMProjection {
  final val MAX_SPINS = 20 // FIXME: This is related to the JDBM ConcurrentMod exception, and should be removed when that's cleaned up
}

// FIXME: Again, related to JDBM concurent mod exception
class VicciniException(message: String) extends java.io.IOException("Inconceivable! " + message)

