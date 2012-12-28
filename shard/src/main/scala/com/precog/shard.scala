package com.precog

import akka.dispatch.Future
import scalaz.StreamT
import blueeyes.json.JValue
import java.nio.CharBuffer


package object shard {
  type QueryResult = Either[JValue, StreamT[Future, CharBuffer]]
}

