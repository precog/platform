package com.precog.shard
package mongo

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.common.jobs.JobManager
import com.precog.standalone.StandaloneShardServer

object MongoShardServer extends StandaloneShardServer {
  val caveatMessage = Some("""
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

  val actorSystem = ActorSystem("mongoExecutorActorSystem")
  implicit val executionContext = actorSystem.dispatcher
  logger.debug("Starting server with context: " + executionContext)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def platformFor(config: Configuration, jobManager: JobManager[Future]) =
    (new MongoQueryExecutor(new MongoQueryExecutorConfig(config.detach("queryExecutor")), jobManager, actorSystem),
     Stoppable.fromFuture(Future(())))
}
