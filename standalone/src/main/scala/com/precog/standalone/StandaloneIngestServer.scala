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
package com.precog.standalone

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import scalaz.Monad

import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.ingest._
import com.precog.ingest.kafka._

import org.streum.configrity.Configuration

object StandaloneIngestServer
    extends BlueEyesServer
    with EventService
    with AkkaDefaults {
  val clock = Clock.System

  def APIKeyFinder(config: Configuration): APIKeyFinder[Future]
    = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey"))
  def AccountFinder(config: Configuration): AccountFinder[Future] = sys.error("todo")
  def EventStore(config: Configuration): EventStore = sys.error("todo")
  def JobManager(config: Configuration): JobManager[Future] = sys.error("todo")

  val executionContext = defaultFutureDispatch
  def asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(asyncContext)
}
