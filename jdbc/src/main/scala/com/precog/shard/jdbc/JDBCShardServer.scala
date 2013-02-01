package com.precog.shard
package jdbc

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.{AkkaTypeClasses, Stoppable}

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.standalone.StandaloneShardServer

object JDBCShardServer extends StandaloneShardServer {
  val caveatMessage = Some("""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Precog for PostgreSQL is a free product that Precog provides to the
PostgreSQL community for doing data analysis on PostgreSQL.

Due to technical limitations, we only recommend the product for
exploratory data analysis. For developers interested in
high-performance analytics on their PostgreSQL data, we recommend our
cloud-based analytics solution and the PostgreSQL data importer, which
can nicely complement existing PostgreSQL installations for
analytic-intensive workloads.

Please note that path globs are not yet supported in Precog for PostgreSQL
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")

  override def hardCodedAccount = Some("fakeaccount")

  val actorSystem = ActorSystem("ExecutorSystem")
  val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def configureShardState(config: Configuration) = M.point {
    BasicShardState(JDBCQueryExecutor(config.detach("queryExecutor"))(asyncContext, M), apiKeyManagerFactory(config.detach("security")), Stoppable.fromFuture(Future(())))
  }
}
