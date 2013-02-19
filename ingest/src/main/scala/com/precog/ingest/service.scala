package com.precog.ingest

import _root_.blueeyes.json._

package object service {
  def cleanPath(string: String): String = "/" + string.split("/").map(_.trim).filter(_.length > 0).mkString("/")

  implicit def jpath2rich(jpath: JPath): RichJPath = new RichJPath(jpath)
  class RichJPath(jpath: JPath) {
    def endsInInfiniteValueSpace = jpath.nodes.exists {
      case JPathField(name) => name startsWith "~"
      case _ => false
    }
  }
}
