package com.precog.standalone

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.json.JValue
import blueeyes.util.Clock

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, HandlerWrapper, ResourceHandler}

import scalaz.Monad

import com.precog.accounts._
import com.precog.common.security._
import com.precog.shard.ShardService
import java.awt.Desktop
import java.net.URI


trait StandaloneShardServer
    extends BlueEyesServer
    with ShardService {
  val clock = Clock.System

  implicit def executionContext: ExecutionContext

  def caveatMessage: Option[String]

  def openBrowser(port: Int){
    Desktop.getDesktop.browse(new URI("http://localhost:%s".format(port)))
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
            response.sendRedirect("http://%1$s:%2$d/index.html?apiKey=%3$s&analyticsService=http://%1$s:%4$d/&version=false".format(requestedHost, serverPort, rootKey, quirrelPort))
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
