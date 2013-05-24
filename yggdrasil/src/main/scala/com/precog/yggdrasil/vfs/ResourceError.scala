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
package vfs

import blueeyes.json.serialization.Extractor
import scalaz._

sealed trait ResourceError {
  def fold[A](fatalError: ResourceError.FatalError => A, userError: ResourceError.UserError => A): A
}
  
object ResourceError {
  implicit val show = Show.showFromToString[ResourceError]

  def corrupt(message: String): ResourceError with FatalError = Corrupt(message)
  def ioError(ex: Throwable): ResourceError with FatalError = IOError(ex)
  def illegalWrite(message: String): ResourceError with UserError = IllegalWriteRequestError(message)
  def permissionsError(message: String): ResourceError with UserError = PermissionsError(message)
  def notFound(message: String): ResourceError with UserError = NotFound(message)

  def all(errors: NonEmptyList[ResourceError]): ResourceError with FatalError with UserError = new ResourceErrors(
    errors flatMap {
      case ResourceErrors(e0) => e0
      case other => NonEmptyList(other)
    }
  )

  def fromExtractorError(msg: String): Extractor.Error => ResourceError = { error =>
    Corrupt("%s:\n%s" format (msg, error.message))
  }

  sealed trait FatalError { self: ResourceError =>
    def fold[A](fatalError: FatalError => A, userError: UserError => A) = fatalError(self)
  }

  sealed trait UserError { self: ResourceError =>
    def fold[A](fatalError: FatalError => A, userError: UserError => A) = userError(self)
  }

  case class Corrupt(message: String) extends ResourceError with FatalError
  case class IOError(exception: Throwable) extends ResourceError with FatalError

  case class IllegalWriteRequestError(message: String) extends ResourceError with UserError
  case class PermissionsError(message: String) extends ResourceError with UserError
  case class NotFound(message: String) extends ResourceError with UserError

  case class ResourceErrors private[ResourceError] (errors: NonEmptyList[ResourceError]) extends ResourceError with FatalError with UserError { self =>
    override def fold[A](fatalError: FatalError => A, userError: UserError => A) = {
      val hasFatal = errors.list.exists(_.fold(_ => true, _ => false))
      if (hasFatal) fatalError(self) else userError(self)
    }
  }
}

