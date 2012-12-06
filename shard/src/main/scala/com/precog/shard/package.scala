package com.precog

import akka.dispatch.Future
import scalaz.StreamT
import blueeyes.json.JValue
import java.nio.CharBuffer

package object shard {
  type QueryResult = Either[JValue, StreamT[Future, CharBuffer]]
  type JobQueryT[M[+_], +A] = QueryT[JobQueryState, M, A]

  object JobQueryT extends QueryTCompanion[JobQueryState] {
    def createJob[M[+_], N[+_]](apiKey: APIKey)(implicit Q: JobQueryStateManager[N], f: N ~> M): JobQueryT[M, Job] = {
      QueryT(f(Q.jobManager.createJob(apiKey) map { job =>
        Managed(job.id, Set.empty, job)
      }))
    }

    def withResource[M[+_], A](acquire: => A)(implicit closeable: Closeable[A]): JobQueryT[M, A] = QueryT(M.point {
      val resource = acquire
      Unmanaged(Set(QueryResource(resource, closeable)), resource)
    })
  }
}

