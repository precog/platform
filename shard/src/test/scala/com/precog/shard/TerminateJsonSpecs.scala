package com.precog.shard

import org.specs2.mutable.Specification

import java.nio.CharBuffer

import blueeyes.json.JParser
import blueeyes.json.ParseException

import scalaz._

class TerminateJsonSpecs extends Specification {
  def parse(partialJson: String) = {
    val stream = CharBuffer.wrap(partialJson) :: StreamT.empty[Need, CharBuffer]
    val terminated = TerminateJson.ensure(stream)
    val json = terminated.foldLeft("")(_ + _.toString).value
    JParser.parseFromString(json)
  }

  def chunkAndParse(partialJson: String) = {
    val stream = StreamT.fromStream(Need {
      partialJson.toStream.map { c => CharBuffer.wrap(c.toString) }
    })
    val terminated = TerminateJson.ensure(stream)
    val json = terminated.foldLeft("")(_ + _.toString).value
    JParser.parseFromString(json)
  }

  "terminating simple JSON" should {
    "return valid JSON for empty string" in {
      parse("") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at unclosed key" in {
      parse("""[{"key""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed key" in {
      parse("""[{"key"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at key colon" in {
      parse("""[{"key":""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at unclosed value" in {
      parse("""[{"key":"val""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed value" in {
      parse("""[{"key":"value"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at comma after value" in {
      parse("""[{"key":"value",""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed object" in {
      parse("""[{"key":"value"}""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at comma after object" in {
      parse("""[{"key":"value"},""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at key after value" in {
      parse("""[{"key":"value","test"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at nested array's object's key" in {
      parse("""[{"key":"value"},[{"key"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at string escape character" in {
      parse("""[{"key":"value\""") must beLike {
        case Success(_) => ok
      }
    }
    "not terminate perfectly fine string ~" in {
      val data = """[{"_id":"ObjectId(\"50c0340042a1295fc55f4783\")"},{"_id":"ObjectId(\"50c0340042a1295fc55f4784\")"}]"""
      parse(data) must beLike {
        case Success(x) => ok
      }
    }
    "not terminate perfectly fine string" in {
      val data = """[{"Name":"HASTINGS Amy","_id":"ObjectId(\"50c0340042a1295fc55f4783\")","Countryname":"US","testthis":true,"Population":311591917,"Sportname":"Track                 and Field","Sex":"F"},{"Name":"SARNOBAT Rahi","_id":"ObjectId(\"50c0340042a1295fc55f4784\")","Countryname":"India","Population":1241491960,"Sportname":"Shooting","Sex":"F"}]"""
      parse(data) must beLike {
        case Success(x) => ok
      }
    }
  }

  "terminating chunked JSON" should {
    "return valid JSON when truncated at unclosed key" in {
      chunkAndParse("""[{"key""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed key" in {
      chunkAndParse("""[{"key"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at key colon" in {
      chunkAndParse("""[{"key":""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at unclosed value" in {
      chunkAndParse("""[{"key":"val""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed value" in {
      chunkAndParse("""[{"key":"value"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at comma after value" in {
      chunkAndParse("""[{"key":"value",""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at closed object" in {
      chunkAndParse("""[{"key":"value"}""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at comma after object" in {
      chunkAndParse("""[{"key":"value"},""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at key after value" in {
      chunkAndParse("""[{"key":"value","test"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at nested array's object's key" in {
      chunkAndParse("""[{"key":"value"},[{"key"""") must beLike {
        case Success(_) => ok
      }
    }
    "return valid JSON when truncated at string escape character" in {
      chunkAndParse("""[{"key":"value\""") must beLike {
        case Success(_) => ok
      }
    }
  }
}

