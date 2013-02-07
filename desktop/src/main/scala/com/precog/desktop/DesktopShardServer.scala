package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.{AkkaTypeClasses, Stoppable}

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.common.jobs.InMemoryJobManager
import com.precog.shard.jdbm3.JDBMQueryExecutorComponent
import com.precog.standalone.StandaloneShardServer

object DesktopShardServer
    extends StandaloneShardServer
    with JDBMQueryExecutorComponent {
  val caveatMessage = None
  override def hardCodedAccount = Some("desktop")

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def configureShardState(config: Configuration) = M.point {
    val apiKeyManager = apiKeyManagerFactory(config.detach("security"))
    val accountManager = accountManagerFactory(config.detach("accounts"))
    val jobManager = new InMemoryJobManager
    val platform = platformFactory(config.detach("queryExecutor"), apiKeyManager, accountManager, jobManager)

    val stoppable = Stoppable.fromFuture {
      for {
        _ <- apiKeyManager.close
        _ <- accountManager.close
        _ <- platform.shutdown
      } yield ()
    }

    ManagedQueryShardState(platform, apiKeyManager, accountManager, jobManager, clock, stoppable)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start JDBM Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
