package com.precog

import akka.dispatch.Future
import scalaz.StreamT
import blueeyes.json.JsonAST.JValue


package object shard {
  type QueryResult = Either[JValue, StreamT[Future, List[JValue]]]

}

