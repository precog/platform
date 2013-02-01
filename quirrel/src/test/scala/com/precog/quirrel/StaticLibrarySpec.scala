package com.precog
package quirrel

import bytecode.{Instructions, StaticLibrary}

trait StaticLibrarySpec extends Instructions {
  type Lib = StaticLibrary
  val library = new StaticLibrary{}
}
