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

import security._
import json._

import blueeyes.json.{ JPath, JValue }
import blueeyes.json.serialization.DefaultSerialization._

import shapeless._

sealed trait Action

case class Event(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: JValue, metadata: Map[JPath, Set[UserMetadata]]) extends Action 

object Event {
  implicit val eventIso = Iso.hlist(Event.apply _, Event.unapply _)
  
  val schema = "apiKey" :: "path" :: "ownerAccountId" :: "data" :: "metadata" :: HNil
  
  implicit val (eventDecomposer, eventExtractor) = serialization[Event](schema)

  def fromJValue(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: JValue): Event = {
    Event(apiKey, path, ownerAccountId, data, Map[JPath, Set[UserMetadata]]())
  }
}

case class Archive(path: Path, apiKey: String) extends Action

object Archive {
  implicit val archiveIso = Iso.hlist(Archive.apply _, Archive.unapply _)

  val schema = "apiKey" :: "path" :: HNil
  
  implicit val (archiveDecomposer, archiveExtractor) = serialization[Archive](schema)
}

// vim: set ts=4 sw=4 et:
