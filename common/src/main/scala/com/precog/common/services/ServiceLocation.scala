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
package com.precog.common
package services

import blueeyes.core.http.URI

import org.streum.configrity.Configuration

import scalaz._
import scalaz.NonEmptyList._
import scalaz.syntax.applicative._
import scalaz.syntax.std.option._

case class ServiceLocation(protocol: String, host: String, port: Int, pathPrefix: Option[String]) {
  def toURI: URI = URI(scheme = Some(protocol), host = Some(host), port = Some(port), path = pathPrefix)
}

object ServiceLocation {
  def fromConfig(conf: Configuration): ValidationNel[String, ServiceLocation] = {
    (conf.get[String]("protocol").toSuccess(nels("Configuration property protocol is required")) |@|
     conf.get[String]("host").toSuccess(nels("Configuration property host is required")) |@|
     conf.get[Int]("port").toSuccess(nels("Configuration property port is required"))) { (protocol, host, port) =>
      ServiceLocation(protocol, host, port, conf.get[String]("pathPrefix"))  
    } 
  }
}




