package com.precog.shard
package jdbm3

//import com.precog.common.security._
//import com.precog.common.security.service._
//import com.precog.common.accounts._
//import com.precog.common.jobs._
//import com.precog.common.client.BaseClient._
//
//import blueeyes.BlueEyesServer
//import blueeyes.bkka._
//import blueeyes.util.Clock
//
//import akka.dispatch.Future
//import akka.dispatch.Promise
//import akka.dispatch.ExecutionContext
//import akka.actor.ActorSystem
//
//import org.streum.configrity.Configuration
//
//import scalaz._
//
//object JDBMShardServer extends BlueEyesServer 
//    with ShardService 
//    with JDBMQueryExecutorComponent {
//  import WebJobManager._
//
//  val clock = Clock.System
//
//  val actorSystem = ActorSystem("PrecogShard")
//  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
//  implicit val M: Monad[Future] = new FutureMonad(executionContext)
//
//  override def configureShardState(config: Configuration) = M.point {
//    val apiKeyFinder = WebAPIKeyFinder(config.detach("security")) valueOr { errs =>
//      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
//    }
//
//    val accountFinder = WebAccountFinder(config.detach("accounts")) valueOr { errs =>
//      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
//    }
//
//    val jobManager = WebJobManager(config.detach("jobs")).withM[Future]
//    val platform = platformFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)
//    val stoppable = Stoppable.fromFuture(platform.shutdown)
//
//    ManagedQueryShardState(platform, apiKeyFinder, jobManager, clock, stoppable)
//  } recoverWith {
//    case ex: Throwable =>
//      System.err.println("Could not start JDBM Shard server!!!")
//      ex.printStackTrace
//      Promise.failed(ex)
//  }
//}
