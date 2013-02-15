package com.precog.ingest.util

import akka.dispatch.{Future, ExecutionContext}

object FutureUtils {
  def singleSuccess[T](futures: Seq[Future[T]])(implicit executor: ExecutionContext): Future[Option[T]] = Future.find(futures){ _ => true }
}
