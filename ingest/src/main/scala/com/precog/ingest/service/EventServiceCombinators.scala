package com.precog.ingest
package service

import com.precog.common.Path
import com.precog.common.accounts.AccountServiceCombinators
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.services.PathServiceCombinators

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

trait EventServiceCombinators extends APIKeyServiceCombinators with PathServiceCombinators {
  import DefaultBijections._

  def left[A, B, C](h: HttpService[Either[A, B], C]): HttpService[A, C] = {
    new CustomHttpService[A, C] {
      val service = (req: HttpRequest[A]) =>
        h.service(req.copy(content = req.content map (Left(_))))

      val metadata = None
    }
  }

  def right[A, B, C](h: HttpService[Either[A, B], C]): HttpService[B, C] = {
    new CustomHttpService[B, C] {
      val service = (req: HttpRequest[B]) =>
        h.service(req.copy(content = req.content map (Right(_))))

      val metadata = None
    }
  }
}

// vim: set ts=4 sw=4 et:
