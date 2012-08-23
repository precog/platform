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
package com.precog.common.json

import blueeyes.json.JsonAST
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import util.matching.Regex

import JsonAST._
import scalaz.Order
import scalaz.Ordering
import scalaz.Ordering._

sealed trait CPath { self =>
  def nodes: List[CPathNode]

  def parent: Option[CPath] = if (nodes.isEmpty) None else Some(CPath(nodes.take(nodes.length - 1): _*))

  def ancestors: List[CPath] = {
    def ancestors0(path: CPath, acc: List[CPath]): List[CPath] = {
      path.parent match {
        case None => acc

        case Some(parent) => ancestors0(parent, parent :: acc)
      }
    }

    ancestors0(this, Nil).reverse
  }

  def \ (that: CPath):  CPath = CPath(self.nodes ++ that.nodes)
  def \ (that: String): CPath = CPath(self.nodes :+ CPathField(that))
  def \ (that: Int):    CPath = CPath(self.nodes :+ CPathIndex(that))

  def \: (that: CPath):  CPath = CPath(that.nodes ++ self.nodes)
  def \: (that: String): CPath = CPath(CPathField(that) +: self.nodes)
  def \: (that: Int):    CPath = CPath(CPathIndex(that) +: self.nodes)

  def hasPrefix(p: CPath): Boolean = nodes.startsWith(p.nodes)

  def dropPrefix(p: CPath): Option[CPath] = {
    def remainder(nodes: List[CPathNode], toDrop: List[CPathNode]): Option[CPath] = {
      nodes match {
        case x :: xs =>
          toDrop match {
            case `x` :: ys => remainder(xs, ys)
            case Nil => Some(CPath(nodes))
            case _ => None
          }

        case Nil => 
          if (toDrop.isEmpty) Some(CPath(nodes)) 
          else None
      }
    }

    remainder(nodes, p.nodes)
  }

  def apply(index: Int): CPathNode = nodes(index)

  def extract(jvalue: JValue): JValue = {
    def extract0(path: List[CPathNode], d: JValue): JValue = path match {
      case Nil => d

      case head :: tail => head match {
        case CPathField(name)  => extract0(tail, d \ name)
        case CPathIndex(index) => extract0(tail, d(index))
      }
    }

    extract0(nodes, jvalue)
  }

  def head: Option[CPathNode] = nodes.headOption

  def tail: CPath = CPath(nodes.tail: _*)

  def expand(jvalue: JValue): List[CPath] = {
    def isRegex(s: String) = s.startsWith("(") && s.endsWith(")")

    def expand0(current: List[CPathNode], right: List[CPathNode], d: JValue): List[CPath] = right match {
      case Nil => CPath(current) :: Nil

      case head :: tail => head match {
        case x @ CPathIndex(index) => expand0(current :+ x, tail, jvalue(index))
        case x @ CPathField(name) if (isRegex(name)) => {
          val regex = name.r

          jvalue.children.flatMap { child =>
            child match {
              case JField(regex(name), value) =>
                val expandedNode = CPathField(name)

                expand0(current :+ expandedNode, tail, value)

              case _ => Nil
            }
          }
        }
        case x @ CPathField(name) => expand0(current :+ x, tail, jvalue \ name)
      }
    }

    expand0(Nil, nodes, jvalue)
  }

  def path = nodes.mkString("")

  def iterator = nodes.iterator

  def length = nodes.length

  override def toString = if (nodes.isEmpty) "." else path
}

sealed trait CPathNode {
  def \(that: CPath) = CPath(this :: that.nodes)
  def \(that: CPathNode) = CPath(this :: that :: Nil)
}

object CPathNode {
  implicit def s2PathNode(name: String): CPathNode = CPathField(name)
  implicit def i2PathNode(index: Int): CPathNode = CPathIndex(index)

  implicit object CPathNodeOrder extends Order[CPathNode] {
    def order(n1: CPathNode, n2: CPathNode): Ordering = (n1, n2) match {
      case (CPathField(s1), CPathField(s2)) => Ordering.fromInt(s1.compare(s2))
      case (CPathField(_) , _             ) => GT
      case (CPathIndex(i1), CPathIndex(i2)) => Ordering.fromInt(i1.compare(i2))
      case (CPathIndex(_) , _             ) => LT
    }
  }

  implicit val CPathNodeOrdering = CPathNodeOrder.toScalaOrdering
}

sealed case class CPathField(name: String) extends CPathNode {
  override def toString = "." + name
}

sealed case class CPathIndex(index: Int) extends CPathNode {
  override def toString = "[" + index + "]"
}

case object CPathArray extends CPathNode {
  override def toString = "[*]"
}

trait CPathSerialization {
  implicit val CPathDecomposer : Decomposer[CPath] = new Decomposer[CPath] {
    def decompose(cpath: CPath) : JValue = JString(cpath.toString)
  }

  implicit val CPathExtractor : Extractor[CPath] = new Extractor[CPath] with ValidatedExtraction[CPath] {
    override def validated(obj : JValue) : scalaz.Validation[Extractor.Error,CPath] =
      obj.validated[String].map(CPath(_))
  }
}

object CPath extends CPathSerialization {
  import blueeyes.json._

  private[this] case class CompositeCPath(nodes: List[CPathNode]) extends CPath 

  private val PathPattern  = """\.|(?=\[\d+\])""".r
  private val IndexPattern = """^\[(\d+)\]$""".r

  val Identity = apply()

  def apply(n: CPathNode*): CPath = CompositeCPath(n.toList)

  def apply(l: List[CPathNode]): CPath = apply(l: _*)

  def apply(path: JPath): CPath = {
    val nodes2 = path.nodes map {
      case JPathField(name) => CPathField(name)
      case JPathIndex(idx) => CPathIndex(idx)
    }

    CPath(nodes2: _*)
  }

  def unapplySeq(path: CPath): Option[List[CPathNode]] = Some(path.nodes)

  def unapplySeq(path: String): Option[List[CPathNode]] = Some(apply(path).nodes)

  implicit def apply(path: String): CPath = {
    def parse0(segments: List[String], acc: List[CPathNode]): List[CPathNode] = segments match {
      case Nil => acc

      case head :: tail =>
        if (head.trim.length == 0) parse0(tail, acc)
        else parse0(tail,
          (head match {
            case IndexPattern(index) => CPathIndex(index.toInt)

            case name => CPathField(name)
          }) :: acc
        )
    }

    val properPath = if (path.startsWith(".")) path else "." + path

    apply(parse0(PathPattern.split(properPath).toList, Nil).reverse: _*)
  }

  implicit def singleNodePath(node: CPathNode) = CPath(node)

  implicit object CPathOrder extends Order[CPath] {
    def order(v1: CPath, v2: CPath): Ordering = {
      def compare0(n1: List[CPathNode], n2: List[CPathNode]): Ordering = (n1, n2) match {
        case (Nil    , Nil)     => EQ
        case (Nil    , _  )     => LT
        case (_      , Nil)     => GT
        case (n1::ns1, n2::ns2) =>
          val ncomp = Order[CPathNode].order(n1, n2)
          if(ncomp != EQ) ncomp else compare0(ns1, ns2)
      }

      compare0(v1.nodes, v2.nodes)
    }
  }

  implicit val CPathOrdering = CPathOrder.toScalaOrdering
}

trait CPathImplicits {
  class StringExtensions(s: String) {
    def cpath = CPath(s)
  }

  implicit def stringExtensions(s: String) = new StringExtensions(s)
}

object CPathImplicits extends CPathImplicits
