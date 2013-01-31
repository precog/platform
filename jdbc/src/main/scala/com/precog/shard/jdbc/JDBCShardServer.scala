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
package jdbc

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import com.precog.common.security._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.json.JValue
import blueeyes.util.Clock

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, ResourceHandler}

import scalaz._

import org.streum.configrity.Configuration

object JDBCShardServer extends BlueEyesServer with ShardService {
  val actorSystem = ActorSystem("ExecutorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new blueeyes.bkka.FutureMonad(executionContext)
  
  val clock = Clock.System

  def configureShardState(config: Configuration) = Future {
    val apiKeyFinder = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey"))
    BasicShardState(
      JDBCQueryExecutor(
        config.detach("queryExecutor"))(executionContext, M), 
        apiKeyFinder,
        Stoppable.fromFuture(Promise.successful(())))
  }

  val jettyService = this.service("labcoat", "1.0") { context =>
    startup {
      val rootConfig = context.rootConfig
      val config = rootConfig.detach("services.quirrel.v1")
      val serverPort = config[Int]("labcoat.port", 8000)
      val quirrelPort = rootConfig[Int]("server.port", 8888)
      val rootKey = config[String]("security.masterAccount.apiKey")

      logger.warn("""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Precog for PostgreSQL is a free product that Precog provides to the
PostgreSQL community for doing data analysis on PostgreSQL.

Due to technical limitations, we only recommend the product for
exploratory data analysis. For developers interested in
high-performance analytics on their PostgreSQL data, we recommend our
cloud-based analytics solution and the PostgreSQL data importer, which
can nicely complement existing PostgreSQL installations for
analytic-intensive workloads.

Please note that path globs are not yet supported in Precog for PostgreSQL
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")

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
            val requestedHost = Option(request.getHeader("Host")).map(_.toLowerCase.split(':').head).getOrElse("localhost")
            response.sendRedirect("http://%1$s:%2$d/index.html?apiKey=%3$s&analyticsService=http://%1$s:%4$d/&version=false&useJsonp=true".format(requestedHost, serverPort, rootKey, quirrelPort))
          }
        }
      }

      val handlers = new HandlerList

      handlers.setHandlers(Array[Handler](rootHandler, resourceHandler, new DefaultHandler))
      server.setHandler(handlers)
      server.start()

      Promise.successful(server)
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
