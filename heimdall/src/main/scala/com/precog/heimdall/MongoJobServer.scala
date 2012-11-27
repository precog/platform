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
package com.precog.heimdall

import akka.dispatch.Future

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.persistence.mongo._
import blueeyes.BlueEyesServer

import org.streum.configrity.Configuration

object MongoJobServer extends BlueEyesServer with JobService with ManagedMongoJobManagerModule {
  implicit val executionContext = defaultFutureDispatch

  val clock = blueeyes.util.Clock.System

  type Resource = Mongo

  def close(mongo: Mongo) = mongo.close

  def authService(config0: Configuration): AuthService[Future] = {
    import WebJobManager._

    val config = config0.detach("auth")
    val protocol = config[String]("protocol", "http")
    val host = config[String]("host", "localhost")
    val port = config[Int]("port", 30062)
    val path = config[String]("path", "auth")

    WebAuthService(protocol, host, port, path).withM[Future]
  }
}

