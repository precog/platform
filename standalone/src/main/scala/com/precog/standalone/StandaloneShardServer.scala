package com.precog.standalone

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.BlueEyesServer
import blueeyes.bkka.AkkaTypeClasses
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.json.JValue
import blueeyes.util.Clock

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, ResourceHandler}

import scalaz.Monad

import com.precog.accounts.StaticAccountManagerClientComponent
import com.precog.common.security.StaticAPIKeyManagerComponent
import com.precog.shard.ShardService


trait StandaloneShardServer
    extends BlueEyesServer
    with ShardService
    with StaticAPIKeyManagerComponent
    with StaticAccountManagerClientComponent {
  val clock = Clock.System

  implicit def asyncContext: ExecutionContext

  def caveatMessage: Option[String]

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

      Future(server)(asyncContext)
    } ->
    request { (server: Server) =>
      get {
        (req: HttpRequest[ByteChunk]) => Promise.successful(HttpResponse[ByteChunk]())(asyncContext)
      }
    } ->
    shutdown { (server: Server) =>
      Future(server.stop())(asyncContext)
    }
  }
}