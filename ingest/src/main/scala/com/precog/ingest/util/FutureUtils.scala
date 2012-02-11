package com.precog.ingest.util

import akka.dispatch.{Future, MessageDispatcher}

object FutureUtils {
  def singleSuccess[T](futures: Seq[Future[T]])(implicit dispatcher: MessageDispatcher): Future[Option[T]] = Future.find(futures){ _ => true }
}
