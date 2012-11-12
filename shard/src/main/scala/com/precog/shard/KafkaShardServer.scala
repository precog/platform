/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package shard

import common.security._
import ingest.service.NullUsageLogging
import shard.yggdrasil.YggdrasilQueryExecutorComponent

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._

import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import org.streum.configrity.Configuration

import scalaz._

object KafkaShardServer extends BlueEyesServer with ShardService with YggdrasilQueryExecutorComponent with MongoAPIKeyManagerComponent {
  
  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}
