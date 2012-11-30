package com.precog.common

package object jobs {
  type JobId = String
  type MessageId = Long
  type StatusId = Long

  trait IdExtractor {
    val NonNegInt = """(\d+)""".r

    def unapply(str: String): Option[Long] = str match {
      case NonNegInt(str) => Some(str.toLong)
      case _ => None
    }
  }

  object StatusId extends IdExtractor
  object MessageId extends IdExtractor
}

