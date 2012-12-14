package com.precog.yggdrasil
package table

import com.precog.yggdrasil.util._

trait StubIdSourceScannerModule {
  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new FreshAtomicIdSource
  }
}
