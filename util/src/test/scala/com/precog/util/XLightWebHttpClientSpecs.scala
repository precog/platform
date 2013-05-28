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
package com.precog.util

import org.specs2.mutable._
import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

import java.util.{concurrent => juc}

object XLightWebHttpClientSpecs extends Specification with XLightWebHttpClientModule[Need] {
  implicit def M = Need.need
  
  "xlightweb http client" should {
    "fetch precog.com" in {
      val client = HttpClient("http://precog.com")
      val response = (client.execute(Request[String]()) | sys.error("...")).copoint
      val data = response.body getOrElse ""
      
      data must contain("Precog")
    }
    
    "submit a google search" in {
      val client = HttpClient("http://www.google.com")
      val request = (Request() / "search") ? ("q" -> "recursion")
      val data = (client.execute(request) | sys.error("...")).copoint.body getOrElse ""
      
      data must contain("Did you mean")
    }
    
    "submit a test post" in {
      val body = "Silly fish had a purple dish; make a wish!"
      val client = HttpClient("http://posttestserver.com")
      val request = (Request(HttpMethod.POST) / "post.php").withBody("text/plain", body)
      val data = (client.execute(request) | sys.error("...")).copoint.body getOrElse ""
      
      data must contain("Post body was %d chars long".format(body.length))
    }

    "bad URL results in error" in {
      val client = HttpClient("asdf")
      client.execute(Request()).isLeft.copoint must beTrue
    }
  }
}
