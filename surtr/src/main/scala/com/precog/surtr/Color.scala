/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.pandora

class Color(val enabled: Boolean) {
  val Bold = "\u001B[1m"
  
  val RedForeground = "\u001B[31m"
  val GreenForeground = "\u001B[33m"
  val YellowForeground = "\u001B[33m"
  val BlueForeground = "\u001B[34m"
  val CyanForeground = "\u001B[36m"
  
  val Reset = "\u001B[0m"
  
  def bold(str: String) = format(Bold, str)
  
  def red(str: String) = format(RedForeground, str)
  def green(str: String) = format(GreenForeground, str)
  def yellow(str: String) = format(YellowForeground, str)
  def blue(str: String) = format(BlueForeground, str)
  def cyan(str: String) = format(CyanForeground, str)
  
  private def format(escape: String, str: String) =
    "%s%s%s".format(escape, str, Reset)
}
