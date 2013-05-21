package com.precog.shard
package nihdb

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.client._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.actor.ActorSystem

import org.streum.configrity.Configuration

import scalaz._

object NIHDBShardServer extends BlueEyesServer
    with ShardService
    with NIHDBQueryExecutorComponent {
  import WebJobManager._

  val clock = Clock.System

  val actorSystem = ActorSystem("PrecogShard")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  override def configureShardState(config: Configuration) = M.point {
    val apiKeyFinder = WebAPIKeyFinder(config.detach("security")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
    }

    val accountFinder = WebAccountFinder(config.detach("accounts")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    }


    val (asyncQueries, jobManager) = {
      if (config[Boolean]("jobs.service.in_memory", false)) {
        (ShardStateOptions.DisableAsyncQueries, ExpiringJobManager[Future](config.detach("jobs")))
      } else {
        WebJobManager(config.detach("jobs")) map { webJobManager =>
          (ShardStateOptions.NoOptions, webJobManager.withM[Future])
        } valueOr { errs => 
          sys.error("Unable to build new WebJobManager: " + errs.list.mkString("\n", "\n", ""))
        }
      }
    }

    val platform = platformFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)
    val stoppable = Stoppable.fromFuture(platform.shutdown)

    ManagedQueryShardState(platform, apiKeyFinder, accountFinder, jobManager, clock, stoppable, asyncQueries)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start NIHDB Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
