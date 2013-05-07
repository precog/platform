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
