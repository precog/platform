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
package com.precog.common

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.syntax.std.boolean._

class Path private (val elements: String*) {
  def components: List[String] = elements.toList
  val path: String = elements.mkString("/", "/", "/").replaceAll("/+", "/")
  val length: Int = elements.length

  lazy val parent: Option[Path] = elements.size match {
    case 0 => None
    case 1 => Some(Path.Root)
    case _ => Some(new Path(elements.init: _*))
  }

  lazy val ancestors: List[Path] = {
    val parentList = parent.toList

    parentList ++ parentList.flatMap(_.ancestors)
  }

  def / (that: Path) = new Path(elements ++ that.elements: _*)
  def - (that: Path): Option[Path] = elements.startsWith(that.elements).option(new Path(elements.drop(that.elements.length): _*))

  def isEqualOrParentOf(that: Path) = that.elements.startsWith(this.elements)

  def isChildOf(that: Path) = elements.startsWith(that.elements) && length > that.length

  def isDirectChildOf(that: Path) =
    elements.startsWith(that.elements) && (length - 1) == that.length

  def rollups(depth: Int): List[Path] = this :: ancestors.take(depth)

  def urlEncode: Path = new Path(elements.map(java.net.URLEncoder.encode(_, "UTF-8")): _*)

  def prefix: Option[Path] = elements.nonEmpty.option(Path(components.init))

  override def equals(that: Any) = that match {
    case Path(`path`) => true
    case _ => false
  }

  override def hashCode = path.hashCode

  override def toString = path
}


object Path {
  implicit val PathDecomposer: Decomposer[Path] = StringDecomposer contramap { (_:Path).toString }
  implicit val PathExtractor: Extractor[Path] = StringExtractor map { Path(_) }

  val Root = new Path()

  private def cleanPath(string: String): String = string.replaceAll("^/|/$", "").replaceAll("/+", "/")

  def apply(path: String): Path = new Path(cleanPath(path).split("/").filterNot(_.trim.isEmpty): _*)

  def apply(elements: List[String]): Path = apply(elements.mkString("/"))

  def unapply(path: Path): Option[String] = Some(path.path)
}
