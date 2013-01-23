package com.precog

import com.precog.daze._
import com.precog.common.security._
import com.precog.common.jobs._

import akka.dispatch.Future
import scalaz.StreamT
import blueeyes.json.JValue
import java.nio.CharBuffer

package object shard {
  type QueryResult = Either[JValue, StreamT[Future, CharBuffer]]

  type JobQueryT[M[+_], +A] = QueryT[JobQueryState, M, A]

  type ShardQuery[+A] = JobQueryT[Future, A]

  type BasicQueryExecutorFactory[M[+_]] = QueryExecutorFactory[M, StreamT[Future, CharBuffer]]
  type AsyncQueryExecutorFactory[M[+_]] = QueryExecutorFactory[M, JobId]
  type SyncQueryExecutorFactory[M[+_]] = QueryExecutorFactory[M, (Option[JobId], StreamT[Future, CharBuffer])]
}
