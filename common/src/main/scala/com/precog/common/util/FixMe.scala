package com.precog.common.util

import java.io.File

object FixMe {
  private val show = new File(System.getProperty("user.home") + "/.fixme.show").exists

  def fixme(msg: String) {
    if(show) println("FIXME: " + msg)
  }
}
