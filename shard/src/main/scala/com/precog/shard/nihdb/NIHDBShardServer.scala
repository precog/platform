package com.precog.shard
package nihdb

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.client._
import com.precog.shard.scheduling._
import com.precog.yggdrasil.vfs._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.actor.{ActorSystem, Props}
import akka.pattern.GracefulStopSupport
import akka.util.Timeout

import java.util.concurrent.TimeUnit

import org.joda.time.Instant
import org.streum.configrity.Configuration

import scalaz._

object NIHDBShardServer extends BlueEyesServer
    with ShardService
    with NIHDBQueryExecutorComponent
    with GracefulStopSupport {
  import WebJobManager._

  val clock = Clock.System

  val actorSystem = ActorSystem("PrecogShard")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  override def configureShardState(config: Configuration) = M.point {
    val apiKeyFinder = new CachingAPIKeyFinder(WebAPIKeyFinder(config.detach("security")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
    })

    val accountFinder = new CachingAccountFinder(WebAccountFinder(config.detach("accounts")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    })

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

    val (scheduleStorage, scheduleStorageStoppable) = MongoScheduleStorage(config.detach("scheduling"))

    val schedulingTimeout = new Timeout(config[Int]("scheduling.timeout_ms", 10000))
    val timestampRequiredAfter = new Instant(config[Long]("ingest.timestamp_required_after", 1363327426906L))
    val permissionsFinder = new PermissionsFinder(apiKeyFinder, accountFinder, timestampRequiredAfter)

    val platform = platformFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)
    val vfs = new ActorVFS(platform.projectionsActor, clock, platform.yggConfig.storageTimeout, platform.yggConfig.storageTimeout) //FIXME: good timeout???

    val scheduleActor = actorSystem.actorOf(Props(new SchedulingActor(jobManager, permissionsFinder, vfs, scheduleStorage, platform, clock)))
    val scheduleActorStoppable = Stoppable.fromFuture(gracefulStop(scheduleActor, schedulingTimeout.duration)(actorSystem))
    val scheduler = new ActorScheduler(scheduleActor, schedulingTimeout)

    val stoppable = scheduleActorStoppable.append(Stoppable.fromFuture(platform.shutdown)).append(scheduleStorageStoppable)
    val storedQueries = new VFSStoredQueries(platform, vfs, scheduler, jobManager, permissionsFinder, clock)

    ShardState(platform, apiKeyFinder, accountFinder, vfs, storedQueries, scheduler, jobManager, clock, stoppable, asyncQueries)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start NIHDB Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
