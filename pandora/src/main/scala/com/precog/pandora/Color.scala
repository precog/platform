package com.precog.pandora

class Color(val enabled: Boolean) {
  val Bold = "\u001B[1m"
  
  val RedForeground = "\u001B[31m"
  val GreenForeground = "\u001B[33m"
  val YellowForeground = "\u001B[33m"
  val CyanForeground = "\u001B[36m"
  
  val Reset = "\u001B[0m"
  
  def bold(str: String) = format(Bold, str)
  
  def red(str: String) = format(RedForeground, str)
  def green(str: String) = format(GreenForeground, str)
  def yellow(str: String) = format(YellowForeground, str)
  def cyan(str: String) = format(CyanForeground, str)
  
  private def format(escape: String, str: String) =
    "%s%s%s".format(escape, str, Reset)
}
