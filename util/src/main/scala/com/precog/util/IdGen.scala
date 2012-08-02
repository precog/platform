package com.precog.util

import java.util.concurrent.atomic.AtomicInteger

class IdGen {
  private[this] val currentId = new AtomicInteger(0)
  
  def nextInt(): Int = currentId.getAndIncrement()
}

// Shared Int could easily overflow: Unshare? Extend to a Long? Different approach? 
object IdGen extends IdGen
