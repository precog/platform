package com.precog.heimdall

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

case class JobServiceState(jobManager: JobManager[Future], clock: Clock)

trait JobService
    extends BlueEyesServiceBuilder
    with AkkaDefaults
    with BijectionsChunkFutureByteArray
    with HttpRequestHandlerCombinators {

  import BijectionsChunkFutureString._
  import BijectionsChunkFutureByteArray._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  def close(): Future[Unit]
  def clock: Clock

  def jobManager(config: Configuration): JobManager[Future]

  implicit val timeout: Timeout = Timeout(30000)

  // Ugh. Why do I need this?
  implicit def byteArray2chunk(r: Future[HttpResponse[Array[Byte]]]): Future[HttpResponse[ByteChunk]] = r map { resp => resp.copy(content = resp.content map (Chunk(_))) }

  val jobService = this.service("jobs", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          Future {
            val jobs = jobManager(config)
            JobServiceState(jobs, clock)
          }
        } ->
        request { case JobServiceState(jobs, clock) =>
          jsonp[ByteChunk] {
            path("/jobs") {
              get(new ListJobsHandler(jobs)) ~
              //checkAPIKey {
                post(new CreateJobHandler(jobs, clock)) ~
              //} ~
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
                path("/messages") {
                  get(new ListChannelsHandler(jobs)) ~
                  path("/'channel") {
                    post(new AddMessageHandler(jobs)) ~
                    get(new ListMessagesHandler(jobs))
                  }
                }
              }
            }
          } ~
          path("/jobs/'jodId/result")(put(new CreateResultHandler(jobs, clock))) ~
          path("/jobs/'jobId/result")(get(new GetResultHandler(jobs)))
        } ->
        shutdown { state =>
          close()
        }
      }
    }
  }
}

