package com.precog.common.accounts

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._

import akka.dispatch.Future

import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json.serialization.DefaultSerialization._

trait AccountServiceCombinators extends HttpRequestHandlerCombinators {
//  def accountId[A, B](accountFinder: AccountFinder[Future])(service: HttpService[A, (APIKeyRecord, Path, AccountId) => Future[B]])(implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) = {
//    new AccountRequiredService[A, B](accountFinder, service)
//  }
}
