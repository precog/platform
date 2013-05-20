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
package com.precog.standalone

import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.util.Duration

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.json.JValue
import blueeyes.util.Clock

import java.io.File
import java.util.concurrent.TimeUnit

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.http.MimeTypes
import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, HandlerWrapper, ResourceHandler}

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.accounts._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.shard._
import com.precog.shard.scheduling.NoopScheduler
import com.precog.yggdrasil.vfs.NoopVFS
import java.awt.Desktop
import java.net.URI


trait StandaloneShardServer
    extends BlueEyesServer
    with ShardService {
  val clock = Clock.System

  implicit def executionContext: ExecutionContext

  def caveatMessage: Option[String]

  def platformFor(config: Configuration, apiKeyManager: APIKeyFinder[Future], jobManager: JobManager[Future]): (ManagedPlatform, Stoppable)

  def apiKeyFinderFor(config: Configuration): APIKeyFinder[Future] = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey"))

  def configureShardState(config: Configuration) = M.point {
    val apiKeyFinder = apiKeyFinderFor(config)
    val jobManager = config.get[String]("jobs.jobdir") map { jobdir =>
      val dir = new File(jobdir)

      if (!dir.isDirectory) {
        throw new Exception("Configured job dir %s is not a directory".format(dir))
      }

      if (!dir.canWrite) {
        throw new Exception("Configured job dir %s is not writeable".format(dir))
      }

      FileJobManager(dir, M)
    } getOrElse {
      new ExpiringJobManager(Duration(config[Int]("jobs.ttl", 300), TimeUnit.SECONDS))
    }

    val (platform, stoppable) = platformFor(config, apiKeyFinder, jobManager)

    // We always want a managed shard now, for better error reporting and Labcoat compatibility
    ShardState(platform,
               apiKeyFinder,
               new StaticAccountFinder[Future]("root", config[String]("security.masterAccount.apiKey")),
               NoopVFS,
               NoopStoredQueries[Future],
               NoopScheduler[Future],
               jobManager,
               Clock.System,
               stoppable)
  }

  val jettyService = this.service("labcoat", "1.0") { context =>
    startup {
      val rootConfig = context.rootConfig
      val config = rootConfig.detach("services.analytics.v2")
      val serverPort = config[Int]("labcoat.port", 8000)
      val quirrelPort = rootConfig[Int]("server.port", 8888)
      val rootKey = config[String]("security.masterAccount.apiKey")

      caveatMessage.foreach(logger.warn(_))

      val server = new Server(serverPort)

      // Jetty doesn't map application/json by default
      val mimeTypes = new MimeTypes
      mimeTypes.addMimeMapping("json", "application/json")

      val resourceHandler = new ResourceHandler
      resourceHandler.setMimeTypes(mimeTypes)
      resourceHandler.setDirectoriesListed(false)
      resourceHandler.setWelcomeFiles(new Array[String](0))
      resourceHandler.setResourceBase(this.getClass.getClassLoader.getResource("web").toString)

      val corsHandler = new HandlerWrapper {
        override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
          response.addHeader("Access-Control-Allow-Origin", "*")
          _handler.handle(target, baseRequest, request, response)
        }
      }

      corsHandler.setHandler(resourceHandler)

      val rootHandler = new AbstractHandler {
        def handle(target: String,
                   baseRequest: Request,
                   request: HttpServletRequest,
                   response: HttpServletResponse): Unit = {
          if (target == "/") {
            val requestedHost = Option(request.getHeader("Host")).map(_.toLowerCase.split(':').head).getOrElse("localhost")
            response.sendRedirect("http://%1$s:%2$d/index.html?apiKey=%3$s&analyticsService=http://%1$s:%4$d/&version=2".format(requestedHost, serverPort, rootKey, quirrelPort))
          }
        }
      }

      val handlers = new HandlerList

      handlers.setHandlers(Array[Handler](rootHandler, resourceHandler, new DefaultHandler))
      corsHandler.setHandler(handlers)

      server.setHandler(corsHandler)
      server.start()

      Future(server)(executionContext)
    } ->
    request { (server: Server) =>
      get {
        (req: HttpRequest[ByteChunk]) => Promise.successful(HttpResponse[ByteChunk]())(executionContext)
      }
    } ->
    shutdown { (server: Server) =>
      Future(server.stop())(executionContext)
    }
  }
}
