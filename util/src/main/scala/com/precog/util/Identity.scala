package com.precog.util

import java.util.concurrent.atomic.AtomicInteger

object IdGen {
  private[this] val currentId = new AtomicInteger(0)
  
  @inline
  def nextInt(): Int = currentId.getAndIncrement()
}
