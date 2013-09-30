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
package com.precog
package bifrost

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
