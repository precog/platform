package com.precog.common.jobs

import akka.dispatch.{ Future, ExecutionContext }

import org.streum.configrity.Configuration

import scalaz._

trait JobManagerClientComponent {
  implicit def asyncContext: ExecutionContext
  implicit def M: Monad[Future]

  def jobManagerFactory(config: Configuration): JobManager[Future] = {
    import WebJobManager._

    WebJobManager(config).withM[Future]
  }
}



