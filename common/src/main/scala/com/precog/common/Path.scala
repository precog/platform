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
import blueeyes.json.JsonAST._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.syntax.std.boolean._

class Path private (val elements: String*) {
  val path = elements.mkString("/", "/", "/").replaceAll("/+", "/")
  val length = elements.length

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

  def isEqualOrParent(that: Path) = !(that - this).isEmpty

  def isChildOf(that: Path) = elements.startsWith(that.elements) && length > that.length

  def rollups(depth: Int): List[Path] = this :: ancestors.take(depth) 

  override def equals(that: Any) = that match {
    case Path(`path`) => true
    case _ => false
  }

  override def hashCode = path.hashCode

  override def toString = path
}

trait PathSerialization {
    final implicit val PathDecomposer = new Decomposer[Path] {
    def decompose(v: Path): JValue = JString(v.toString)
  }

  final implicit val PathExtractor = new Extractor[Path] {
    def extract(v: JValue): Path = Path(v.deserialize[String])
  }
}

object Path extends PathSerialization {
  val Root = new Path()

  private def cleanPath(string: String): String = string.replaceAll("^/|/$", "").replaceAll("/+", "/")

  def apply(path: String): Path = new Path(cleanPath(path).split("/").filterNot(_.trim.isEmpty): _*)

  def apply(elements: List[String]): Path = apply(elements.mkString("/"))

  def unapply(path: Path): Option[String] = Some(path.path)
}
