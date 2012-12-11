package com.precog.accounts

import akka.dispatch.{ Future, MessageDispatcher }

import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json.serialization.DefaultSerialization._

import com.precog.common.Path
import com.precog.common.security._

trait AccountServiceCombinators extends HttpRequestHandlerCombinators {

  def accountId[A, B](accountManager: BasicAccountManager[Future])(service: HttpService[A, (APIKeyRecord, Path, AccountId) => Future[B]])
    (implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) = {
    new AccountRequiredService[A, B](accountManager, service)
  }
}
