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
package mongo

import akka.dispatch.{ExecutionContext, Future, Promise}

import com.precog.common.security._

import blueeyes.BlueEyesServer
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.json.JValue
import blueeyes.util.Clock

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, ResourceHandler}

import org.streum.configrity.Configuration

object MongoShardServer extends BlueEyesServer with ShardService with MongoQueryExecutorComponent with StaticAPIKeyManagerComponent {
  
  val clock = Clock.System

  val jettyService = this.service("labcoat", "1.0") { context =>
    startup {
      val rootConfig = context.rootConfig
      val config = rootConfig.detach("services.quirrel.v1")
      val serverPort = config[Int]("labcoat.port", 8000)
      val quirrelPort = rootConfig[Int]("server.port", 8888)
      val rootKey = config[String]("security.masterAccount.apiKey")

      val server = new Server(serverPort)
      val resourceHandler = new ResourceHandler
      resourceHandler.setDirectoriesListed(false)
      resourceHandler.setWelcomeFiles(new Array[String](0))
      resourceHandler.setResourceBase(this.getClass.getClassLoader.getResource("web").toString)

      val rootHandler = new AbstractHandler {
        def handle(target: String,
                   baseRequest: Request,
                   request: HttpServletRequest,
                   response: HttpServletResponse): Unit = {
          if (target == "/") {
            response.sendRedirect("http://localhost:%d/index.html?apiKey=%s&analyticsService=http://localhost:%d/&version=false&useJsonp=true".format(serverPort, rootKey, quirrelPort))
          }
        }
      }

      val handlers = new HandlerList

      handlers.setHandlers(Array[Handler](rootHandler, resourceHandler, new DefaultHandler))
      server.setHandler(handlers)
      server.start()

      Future(server)
    } -> 
    request { (server: Server) =>
      get {
        (req: HttpRequest[ByteChunk]) => Promise.successful(HttpResponse[ByteChunk]())
      }
    } ->
    shutdown { (server: Server) =>
      Future(server.stop())
    }
  }
}
