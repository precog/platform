package com.precog
package quirrel

import bytecode.{Instructions, RandomLibrary}

trait RandomLibrarySpec extends Instructions {
  type Lib = RandomLibrary
  val library = new RandomLibrary{}
}
