package com.precog.yggdrasil
package vfs

import blueeyes.json.serialization.Extractor
import scalaz._
import scalaz.NonEmptyList._

sealed trait ResourceError {
  def fold[A](fatalError: ResourceError.FatalError => A, userError: ResourceError.UserError => A): A
  def messages: NonEmptyList[String]
}
  
object ResourceError {
  implicit val semigroup = new Semigroup[ResourceError] {
    def append(e1: ResourceError, e2: => ResourceError) = all(nels(e1, e2))
  }

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
    def messages: NonEmptyList[String] 
  }

  sealed trait UserError { self: ResourceError =>
    def fold[A](fatalError: FatalError => A, userError: UserError => A) = userError(self)
    def messages: NonEmptyList[String] 
  }

  case class Corrupt(message: String) extends ResourceError with FatalError {
    def messages = nels(message)
  }
  case class IOError(exception: Throwable) extends ResourceError with FatalError {
    def messages = nels(Option(exception.getMessage).getOrElse(exception.getClass.getName))
  }

  case class IllegalWriteRequestError(message: String) extends ResourceError with UserError {
    def messages = nels(message)
  }
  case class PermissionsError(message: String) extends ResourceError with UserError {
    def messages = nels(message)
  }

  case class NotFound(message: String) extends ResourceError with UserError {
    def messages = nels(message)
  }

  case class ResourceErrors private[ResourceError] (errors: NonEmptyList[ResourceError]) extends ResourceError with FatalError with UserError { self =>
    override def fold[A](fatalError: FatalError => A, userError: UserError => A) = {
      val hasFatal = errors.list.exists(_.fold(_ => true, _ => false))
      if (hasFatal) fatalError(self) else userError(self)
    }

    def messages: NonEmptyList[String] = errors.flatMap(_.messages)
  }
}

