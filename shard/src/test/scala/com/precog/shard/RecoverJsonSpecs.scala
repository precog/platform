package com.precog
package shard

import org.specs2.mutable.Specification

import java.nio.CharBuffer

import scalaz.Success
import blueeyes.json.JParser
import blueeyes.json.ParseException

class RecoverJsonSpecs extends Specification {
  def parseClosed(partialJson: String) = {
    val buffer = CharBuffer.wrap(partialJson)
    val closer = RecoverJson.getJsonCloserBuffer(Vector(buffer))
    JParser.parseFromString(partialJson ++ closer.toString)
  }

  "recover JSON" should {
    "return valid JSON when truncated at unclosed key" in {
      parseClosed("""[{"key""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed key" in {
      parseClosed("""[{"key"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at key colon" in {
      parseClosed("""[{"key":""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at unclosed value" in {
      parseClosed("""[{"key":"val""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed value" in {
      parseClosed("""[{"key":"value"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at comma after value" in {
      parseClosed("""[{"key":"value",""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed object" in {
      parseClosed("""[{"key":"value"}""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at comma after object" in {
      parseClosed("""[{"key":"value"},""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at key after value" in {
      parseClosed("""[{"key":"value","test"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at nested array's object's key" in {
      parseClosed("""[{"key":"value"},[{"key"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at string escape character" in {
      parseClosed("""[{"key":"value\""") must beLike {
        case Success(_) => ok
      }
    }
  }
}
