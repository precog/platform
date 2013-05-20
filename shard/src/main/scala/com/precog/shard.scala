package com.precog

import com.precog.daze._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table.Slice

import akka.dispatch.Future
import scalaz.StreamT
import blueeyes.json.JValue
import java.nio.CharBuffer

package object shard {
  type QueryResult = Either[JValue, StreamT[Future, CharBuffer]]

  type JobQueryT[M[+_], +A] = QueryT[JobQueryState, M, A]
  type JobQueryTF[+A] = JobQueryT[Future, A]

  type BasicPlatform[M[+_]] = Platform[M, StreamT[Future, Slice]]
  type AsyncPlatform[M[+_]] = Platform[M, JobId]
  type SyncPlatform[M[+_]] = Platform[M, (Option[JobId], StreamT[Future, Slice])]
}
