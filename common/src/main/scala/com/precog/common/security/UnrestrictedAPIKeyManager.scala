package com.precog.common
package security

import org.joda.time.DateTime

import blueeyes.util.Clock

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

class UnrestrictedAPIKeyManager[M[+_]: Monad](clock: Clock) extends InMemoryAPIKeyManager[M](clock) {
  override def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] = true.point[M]
}
