package com.precog.shard
package mongo

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.{AkkaTypeClasses, Stoppable}

import scalaz.Monad

import org.streum.configrity.Configuration

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

  override def hardCodedAccount = Some("mongo")

  val actorSystem = ActorSystem("mongoExecutorActorSystem")
  val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)


  def configureShardState(config: Configuration) = M.point {
    BasicShardState(MongoQueryExecutor(config.detach("queryExecutor"))(asyncContext, M), apiKeyManagerFactory(config.detach("security")), Stoppable.fromFuture(Future(())))
  }
}