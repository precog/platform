package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import scalaz.Scalaz._

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

  lazy val parentChildRelations: List[(Path, Path)] = ancestors.zip(this :: ancestors)

  def / (that: Path) = new Path(elements ++ that.elements: _*)
  def - (that: Path): Option[Path] = elements.startsWith(that.elements).option(new Path(elements.drop(that.elements.length): _*))
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

  implicit def apply(path: String): Path = new Path(cleanPath(path).split("/").filterNot(_.trim.isEmpty): _*)

  def apply(elements: List[String]): Path = apply(elements.mkString("/"))

  def unapply(path: Path): Option[String] = Some(path.path)
}
