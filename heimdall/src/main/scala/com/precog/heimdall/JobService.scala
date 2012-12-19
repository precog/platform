package com.precog.heimdall

import com.precog.common.jobs._

import akka.dispatch.Future

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.health.metrics.{eternity}
import blueeyes.util.Clock

import akka.util.Timeout

import org.streum.configrity.Configuration

trait JobService
    extends BlueEyesServiceBuilder
    with AkkaDefaults
    with HttpRequestHandlerCombinators {

  import DefaultBijections._

  def clock: Clock

  type Resource

  def close(res: Resource): Future[Unit]

  case class JobServiceState(resource: Resource, jobManager: JobManager[Future], authService: AuthService[Future], clock: Clock)

  def authService(config: Configuration): AuthService[Future]

  def jobManager(config: Configuration): (Resource, JobManager[Future])

  implicit val timeout: Timeout = Timeout(30000)

  // Ugh. Why do I need this?
  implicit def byteArray2chunk(r: Future[HttpResponse[Array[Byte]]]): Future[HttpResponse[ByteChunk]] = r map { resp => resp.copy(content = resp.content map (ByteChunk(_))) }

  val jobService = this.service("jobs", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          Future {
            val (resource, jobs) = jobManager(config)
            JobServiceState(resource, jobs, authService(config), clock)
          }
        } ->
        request { case JobServiceState(_, jobs, auth, clock) =>
          path("/jobs/") {
            path("'jobId") {
              path("/result") {
                put(new CreateResultHandler(jobs)) ~
                get(new GetResultHandler(jobs))
              }
            }
          } ~
          jsonp[ByteChunk] {
            path("/jobs/") {
              get(new ListJobsHandler(jobs)) ~
              post(new CreateJobHandler(jobs, auth, clock)) ~
              path("'jobId") {
                get(new GetJobHandler(jobs)) ~
                path("/status") {
                  get(new GetJobStatusHandler(jobs)) ~
                  put(new UpdateJobStatusHandler(jobs))
                } ~
                path("/state") {
                  get(new GetJobStateHandler(jobs)) ~
                  put(new PutJobStateHandler(jobs))
                } ~
                path("/messages/") {
                  get(new ListChannelsHandler(jobs)) ~
                  path("'channel") {
                    post(new AddMessageHandler(jobs)) ~
                    get(new ListMessagesHandler(jobs))
                  }
                }
              }
            }
          }
        } ->
        shutdown { state =>
          close(state.resource)
        }
      }
    }
  }
}

