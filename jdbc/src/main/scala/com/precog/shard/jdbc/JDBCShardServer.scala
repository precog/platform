package com.precog.shard
package jdbc

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.standalone.StandaloneShardServer

import com.precog.common.security._

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

  val actorSystem = ActorSystem("ExecutorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def configureShardState(config: Configuration) = M.point {
    val apiKeyFinder = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey"))
    BasicShardState(JDBCQueryExecutor(config.detach("queryExecutor")), apiKeyFinder, Stoppable.fromFuture(Future(())))
  }
}
