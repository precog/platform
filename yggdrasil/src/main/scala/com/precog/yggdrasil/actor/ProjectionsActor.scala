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
package com.precog.yggdrasil
package actor

import metadata._
import com.precog.util._
import com.precog.common._
import com.precog.common.ingest._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.{Duration, DurationLong, Timeout}
import akka.util.duration._

import blueeyes.json._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import org.slf4j._

import java.io.File
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.id._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

case class InsertComplete(path: Path)
case class ArchiveComplete(path: Path)
