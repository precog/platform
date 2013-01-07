package com.precog.common.json

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import util.matching.Regex

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

  def combine(paths: Seq[CPath]): Seq[CPath] = {
    if (paths.isEmpty) Seq(this)
    else paths map { path => CPath(this.nodes ++ path.nodes) }
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
          val R = name.r
          jvalue match {
            case JObject(fields) => 
              fields.toList.flatMap { 
                case (R(name), value) =>
                  val expandedNode = CPathField(name)
                  expand0(current :+ expandedNode, tail, value)

                case _ => Nil
              }

            case _ => Nil
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
      case (CPathField(_) , _) => GT
      case (_, CPathField(_)) => LT

      case (CPathArray, CPathArray) => EQ
      case (CPathArray, _) => GT
      case (_, CPathArray) => LT

      case (CPathIndex(i1), CPathIndex(i2)) => if (i1 < i2) LT else if (i1 == i2) EQ else GT
      case (CPathIndex(_), _) => GT
      case (_, CPathIndex(_)) => LT

      case (CPathMeta(m1) , CPathMeta(m2) ) => Ordering.fromInt(m1.compare(m2))
    }
  }

  implicit val CPathNodeOrdering = CPathNodeOrder.toScalaOrdering
}

sealed case class CPathField(name: String) extends CPathNode {
  override def toString = "." + name
}

sealed case class CPathMeta(name: String) extends CPathNode {
  override def toString = "@" + name
}

sealed case class CPathIndex(index: Int) extends CPathNode {
  override def toString = "[" + index + "]"
}

case object CPathArray extends CPathNode {
  override def toString = "[*]"
}

trait CPathSerialization {
  implicit val CPathDecomposer: Decomposer[CPath] = new Decomposer[CPath] {
    def decompose(cpath: CPath): JValue = JString(cpath.toString)
  }

  implicit val CPathExtractor: Extractor[CPath] = new Extractor[CPath] with ValidatedExtraction[CPath] {
    override def validated(obj: JValue): scalaz.Validation[Extractor.Error,CPath] =
      obj.validated[String].map(CPath(_))
  }
}

object CPath extends CPathSerialization {
  import blueeyes.json._

  private[this] case class CompositeCPath(nodes: List[CPathNode]) extends CPath 

  private val PathPattern  = """\.|(?=\[\d+\])|(?=\[\*\])""".r
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
            case "[*]" => CPathArray
            case IndexPattern(index) => CPathIndex(index.toInt)

            case name => CPathField(name)
          }) :: acc
        )
    }

    val properPath = if (path.startsWith(".")) path else "." + path

    apply(parse0(PathPattern.split(properPath).toList, Nil).reverse: _*)
  }

  trait CPathTree[A]
  case class RootNode[A](children: Seq[CPathTree[A]]) extends CPathTree[A]
  case class FieldNode[A](field: CPathField, children: Seq[CPathTree[A]]) extends CPathTree[A]
  case class IndexNode[A](index: CPathIndex, children: Seq[CPathTree[A]]) extends CPathTree[A]
  case class LeafNode[A](value: A) extends CPathTree[A]
  
  case class PathWithLeaf[A](path: Seq[CPathNode], value: A) {
    val size: Int = path.length
  }
  
  def makeTree[A](cpaths0: Seq[CPath], values: Seq[A]): CPathTree[A] = {
    val cpathNodes = cpaths0.sorted map { _.nodes }
    val cpathWithValue = cpathNodes.zip(values) map { case (path, value) => PathWithLeaf[A](path, value) }

    def inner[A](paths: Seq[PathWithLeaf[A]]): Seq[CPathTree[A]] = {
      if (paths.size == 1 && paths.head.size == 0) {
        List(LeafNode(paths.head.value))
      } else {
        val filtered = paths filterNot { case PathWithLeaf(path, _) => path.isEmpty }
        val grouped = filtered groupBy { case PathWithLeaf(path, _) => path.head }

        def recurse[A](paths: Seq[PathWithLeaf[A]]) = 
          inner(paths map { case PathWithLeaf(path, v) => PathWithLeaf(path.tail, v) })

        val result = grouped.toSeq.sortBy(_._1) map { case (node, paths) =>
          node match {
            case (field: CPathField) => FieldNode(field, recurse(paths))
            case (index: CPathIndex) => IndexNode(index, recurse(paths))
            case _ => sys.error("CPathArray and CPathMeta not supported")
          }
        }
        result
      }
    }

    RootNode(inner(cpathWithValue))
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

