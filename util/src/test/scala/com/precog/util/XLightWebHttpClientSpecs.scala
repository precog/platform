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
