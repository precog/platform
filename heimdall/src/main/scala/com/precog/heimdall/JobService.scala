package com.precog.heimdall

import com.precog.common.jobs._
import com.precog.common.services.CORSHeaderHandler

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.RestPathPattern._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.bkka.Stoppable
import blueeyes.health.metrics.{eternity}
import blueeyes.util.Clock
import ByteChunk._
import DefaultBijections._


import akka.dispatch.Promise
import akka.util.Timeout

import org.streum.configrity.Configuration
import scalaz._

// Mongo-backed job manager that doesn't manage its mongo client.
// Mongo-backed job manager that does manage its mongo client.
// In-memory job manager that doesn't do anything.
// REST-backed job manager that doesn't do anything.

// Job service needs to shutdown whatever at the end.

trait JobService extends BlueEyesServiceBuilder with HttpRequestHandlerCombinators {

  def clock: Clock

  type JobResource

  def close(res: JobResource): Future[Unit]

  case class JobServiceState(resource: JobResource, jobManager: JobManager[Future], authService: AuthService[Future], clock: Clock)

  def authService(config: Configuration): AuthService[Future]

  def jobManager(config: Configuration): (JobResource, JobManager[Future])

  implicit val timeout: Timeout = Timeout(30000)

  // Ugh. Why do I need this?
  implicit def byteArray2chunk(r: Future[HttpResponse[Array[Byte]]]): Future[HttpResponse[ByteChunk]] = r map { resp => resp.copy(content = resp.content map (ByteChunk(_))) }

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  val jobService = this.service("jobs", "1.0") {
    requestLogging(timeout) { help("/docs/api") {
      healthMonitor("/health", timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          M.point {
            val (resource, jobs) = jobManager(config)
            JobServiceState(resource, jobs, authService(config), clock)
          }
        } ->
        request { case JobServiceState(_, jobs, auth, clock) =>
          import CORSHeaderHandler.allowOrigin
          allowOrigin("*", executionContext) {
            // Content type is set by the ResultHandler
            path("/jobs/") {
              path("'jobId") {
                path("/result") {
                  put(new CreateResultHandler(jobs)) ~
                  get(new GetResultHandler(jobs))
                }
              }
            } ~
            jsonp {
              produce(MimeTypes.application / MimeTypes.json) {
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
            }
          }
        } ->
        shutdown { state =>
          close(state.resource)
        }
      }
    }}
  }
}

