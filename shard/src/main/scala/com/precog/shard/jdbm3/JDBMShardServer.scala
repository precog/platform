package com.precog.shard
package jdbm3

import com.precog.auth.MongoAPIKeyManagerComponent
import com.precog.accounts.AccountManagerClientComponent
import com.precog.common.security._
import com.precog.common.jobs._

import akka.dispatch.Future
import akka.dispatch.Promise

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import org.streum.configrity.Configuration

import scalaz._

object JDBMShardServer extends BlueEyesServer
    with ShardService
    with JDBMQueryExecutorComponent
    with MongoAPIKeyManagerComponent
    with AccountManagerClientComponent {
  import WebJobManager._

  val clock = Clock.System

  implicit val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  override def configureShardState(config: Configuration) = M.point {
    val apiKeyManager = apiKeyManagerFactory(config.detach("security"))
    val accountManager = accountManagerFactory(config.detach("accounts"))
    val jobManager = {
      if (config[Boolean]("jobs.service.in_memory", false)) {
        ExpiringJobManager[Future](config.detach("jobs"))
      } else {
        WebJobManager(config.detach("jobs")).withM[Future]
      }
    }
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
