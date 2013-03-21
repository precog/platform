package com.precog.common
package security

import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

trait AccessControl[M[+_]] {
  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean]
}

class UnrestrictedAccessControl[M[+_]: Applicative] extends AccessControl[M] {
  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] = true.point[M]
}
