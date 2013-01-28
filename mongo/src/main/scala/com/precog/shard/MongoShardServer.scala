package com.precog.shard
package mongo

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

object MongoShardServer extends BlueEyesServer with ShardService with StaticAPIKeyManagerComponent {
  val actorSystem = ActorSystem("mongoExecutorActorSystem")
  val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  val futureMonad: Monad[Future] = new blueeyes.bkka.FutureMonad(asyncContext)

  val clock = Clock.System

  def configureShardState(config: Configuration) = futureMonad.point {
    BasicShardState(MongoQueryExecutor(config.detach("queryExecutor"))(asyncContext, futureMonad), apiKeyManagerFactory(config.detach("security")), Stoppable.fromFuture(Future(())))
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
Precog for MongoDB is a free product that Precog provides to the
MongoDB community for doing data analysis on MongoDB.

Due to technical limitations, we only recommend the product for
exploratory data analysis. For developers interested in
high-performance analytics on their MongoDB data, we recommend our
cloud-based analytics solution and the MongoDB data importer, which
can nicely complement existing MongoDB installations for
analytic-intensive workloads.

Please note that path globs are not yet supported in Precog for MongoDB
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
            val requestedHost = Option(request.getHeader("Host")).map(_.toLowerCase).getOrElse("localhost")
            response.sendRedirect("http://%1$s:%2$d/index.html?apiKey=%3$s&analyticsService=http://%1$s:%4$d/&version=false&useJsonp=true".format(requestedHost, serverPort, rootKey, quirrelPort))
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
