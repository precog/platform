package com.precog.yggdrasil
package table

import com.precog.yggdrasil.util._

trait StubIdSourceScannerModule {
  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}