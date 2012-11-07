package com.precog.shard
package mongo

import akka.dispatch.Future

import com.precog.common.security._
import com.precog.ingest.service.NullUsageLogging

import blueeyes.BlueEyesServer
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.json.JsonAST.JValue
import blueeyes.util.Clock

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.server.{Handler, Request, Server}
import org.eclipse.jetty.server.handler.{AbstractHandler, DefaultHandler, HandlerList, ResourceHandler}

import org.streum.configrity.Configuration

object MongoShardServer extends BlueEyesServer with ShardService with MongoQueryExecutorComponent with StaticAPIKeyManagerComponent {
  
  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  val asyncContext = defaultFutureDispatch

  val jettyService = this.service("labcoat", "1.0") { context =>
    startup {
      val config = rootConfig.detach("services.quirrel.v1")
      val serverPort = config[Int]("labcoat.port", 8000)
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
            response.sendRedirect("http://localhost:%d/index.html?apiKey=%s&analyticsService=http://localhost:%d/&version=false&useJsonp=true".format(serverPort, rootKey, port))
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
        (req: HttpRequest[ByteChunk]) => Future { HttpResponse[ByteChunk]() }
      }
    } ->
    shutdown { (server: Server) =>
      Future(server.stop())
    }
  }
}
