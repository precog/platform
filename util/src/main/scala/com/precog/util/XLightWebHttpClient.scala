package com.precog.util

import org.xlightweb._
import org.xlightweb.client.{HttpClient => XHttpClient, _}
import java.util.concurrent.Future

import scalaz.syntax.monad._

trait XLightWebHttpClientModule[M[+_]] extends HttpClientModule[M] {
  
  def liftJUCFuture[A](f: Future[A]): M[A]
  
  def HttpClient(host: String): M[HttpClient] =
    M point (new HttpClient(host))
  
  class HttpClient(host: String) extends HttpClientLike {
    private val client = new XHttpClient
    
    def get(path: String, params: (String, String)*): M[Option[String]] = {
      val get = new GetRequest(buildUrl(path))
      
      params foreach {
        case (key, value) => get.setParameter(key, value)
      }
      
      postProcessResp(client.send(get))
    }
    
    def post(path: String, contentType: String, body: String, params: (String, String)*): M[Option[String]] = {
      val post = new PostRequest(buildUrl(path), contentType, body)
      
      params foreach {
        case (key, value) => post.setParameter(key, value)
      }
      
      postProcessResp(client.send(post))
    }
    
    private[this] def buildUrl(path: String) =
      "http://%s/%s".format(host, path)
    
    private[this] def postProcessResp(respF: Future[IHttpResponse]): M[Option[String]] = {
      val respM = liftJUCFuture(respF)
      
      for (resp <- respM) yield {
        if (resp.getStatus >= 200 && resp.getStatus < 300)      // better way to do this
          Option(resp.getBody.readString)
        else
          None
      }
    }
  }
}
