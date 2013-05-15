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
