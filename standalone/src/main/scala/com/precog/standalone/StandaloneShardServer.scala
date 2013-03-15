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

import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, ResourceHandler}

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.accounts._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.shard._


trait StandaloneShardServer
    extends BlueEyesServer
    with ShardService {
  val clock = Clock.System

  implicit def executionContext: ExecutionContext

  def caveatMessage: Option[String]

  def platformFor(config: Configuration, jobManager: JobManager[Future]): (ManagedPlatform, Stoppable)

  def apiKeyFinderFor(config: Configuration): APIKeyFinder[Future] = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey"))

  def configureShardState(config: Configuration) = M.point {
    val apiKeyFinder = apiKeyFinderFor(config)
    config.get[String]("jobs.jobdir").map { jobdir =>
      val dir = new File(jobdir)

      if (!dir.isDirectory) {
        throw new Exception("Configured job dir %s is not a directory".format(dir))
      }

      if (!dir.canWrite) {
        throw new Exception("Configured job dir %s is not writeable".format(dir))
      }

      val jobManager = new FileJobManager(dir, M)
      val (platform, stoppable) = platformFor(config, jobManager)
      ManagedQueryShardState(platform, apiKeyFinder, jobManager, Clock.System, stoppable)
    }.getOrElse {
      val jobManager = new ExpiringJobManager(Duration(60, TimeUnit.SECONDS))
      val (platform, stoppable) = platformFor(config, jobManager)
      BasicShardState(platform, apiKeyFinder, stoppable)
    }
  }

  val jettyService = this.service("labcoat", "1.0") { context =>
    startup {
      val rootConfig = context.rootConfig
      val config = rootConfig.detach("services.quirrel.v1")
      val serverPort = config[Int]("labcoat.port", 8000)
      val quirrelPort = rootConfig[Int]("server.port", 8888)
      val rootKey = config[String]("security.masterAccount.apiKey")

      caveatMessage.foreach(logger.warn(_))

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
