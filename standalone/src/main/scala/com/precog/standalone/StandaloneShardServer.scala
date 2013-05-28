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
import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.accounts._
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

  def configureShardState(config: Configuration) = M.point {
    val apiKey = config[String]("security.masterAccount.apiKey")
    val apiKeyFinder = new StaticAPIKeyFinder[Future](apiKey)
    val accountFinder = new StaticAccountFinder[Future]("root", apiKey, Some("/"))

    val jobManager = config.get[String]("jobs.jobdir").map { jobdir =>
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
               accountFinder,
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
