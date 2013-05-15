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
      val resultsM = for {
        client <- HttpClient("precog.com")
        data <- client.get("")
      } yield data getOrElse ""
      
      resultsM.copoint must contain("Precog")
    }
    
    "submit a google search" in {
      val resultsM = for {
        client <- HttpClient("www.google.com")
        data <- client.get("/search", "q" -> "recursion")
      } yield data getOrElse ""
      
      resultsM.copoint must contain("Did you mean")
    }
    
    "submit a test post" in {
      val body = "Silly fish had a purple dish; make a wish!"
      
      val resultsM = for {
        client <- HttpClient("posttestserver.com")
        data <- client.post("/post.php", "text/plain", body)
      } yield data getOrElse ""
      
      resultsM.copoint must contain("Post body was %d chars long".format(body.length))
    }
  }
  
  def liftJUCFuture[A](f: juc.Future[A]): Need[A] =
    M point { f.get() }     // yay!
}
