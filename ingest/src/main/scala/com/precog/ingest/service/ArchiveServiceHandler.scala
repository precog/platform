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
package ingest
package service

import common._
import common.security._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import scalaz.{Validation, Success}

class ArchiveServiceHandler[A](accessControl: AccessControl[Future], eventStore: EventStore, archiveTimeout: Timeout)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[A, (APIKeyRecord, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => {
    Success { (t: APIKeyRecord, p: Path) =>
      accessControl.mayAccess(t.tid, p, Set(), OwnerPermission) flatMap { mayAccess =>
        if(mayAccess) {
          try { 
            val archiveInstance = Archive(p, t.tid)
            logger.trace("Archiving path: " + archiveInstance)
            eventStore.save(archiveInstance, archiveTimeout).map {
              _ => HttpResponse[JValue](OK)
            }
          } catch {
            case ex => {
              logger.error("Error during archive", ex)
              Future(HttpResponse[JValue](ServiceUnavailable))
            }
          }
        } else {
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your API key does not have permissions to archive this path."))))
        }
      }
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to archive the data under the given path.
    """
  ))
}
