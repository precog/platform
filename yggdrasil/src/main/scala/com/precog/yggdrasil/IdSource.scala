package com.precog.yggdrasil

trait IdSource {
  def nextIdBlock(n: Int): Long
  def nextId(): Long
}

final class FreshAtomicIdSource extends IdSource {
  private val source = new java.util.concurrent.atomic.AtomicLong
  def nextId() = source.getAndIncrement
  def nextIdBlock(n: Int): Long = {
    var nextId = source.get()
    while (!source.compareAndSet(nextId, nextId + n)) {
      nextId = source.get()
    }
    nextId
  }
}

// vim: set ts=4 sw=4 et:
