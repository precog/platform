package com.precog

package object common {
  type ProducerId = Int
  type SequenceId = Int

  final class StringExtensions(s: String) {
    def cpath = CPath(s)
  }

  implicit def stringExtensions(s: String) = new StringExtensions(s)
}


// vim: set ts=4 sw=4 et:
