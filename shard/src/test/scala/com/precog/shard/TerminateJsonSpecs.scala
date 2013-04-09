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

