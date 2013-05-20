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
package security

import accounts._

import com.precog.common.security.service._

import org.specs2.mutable.Specification

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._
import scalaz.syntax.traverse._
import scalaz.std.list._

trait APIKeyManagerSpec[M[+_]] extends Specification {
  implicit def M: Monad[M] with Comonad[M]

  "API Key Manager" should {
    "properly ascribe parentage for grants" in {
      val path = Path("/user1/")
      val mgr = new InMemoryAPIKeyManager[M](blueeyes.util.Clock.System)

      val grantParentage = for {
        rootKey <- mgr.rootAPIKey
        rootGrantId <- mgr.rootGrantId
        perms = Account.newAccountPermissions("012345", Path("/012345/"))
        grantRequest = v1.NewGrantRequest(Some("testGrant"), None, Set(rootGrantId), perms, None)
        record <- mgr.newAPIKeyWithGrants(Some("test"), None, rootKey, Set(grantRequest))
        grants <- record.toList.flatMap(_.grants).map(mgr.findGrant).sequence
      } yield {
        (grants.flatten.flatMap(_.parentIds), rootGrantId)
      }

      val (grantParents, rootGrantId) = grantParentage.copoint

      grantParents must not be empty
      grantParents must haveAllElementsLike({ case gid => gid must_== rootGrantId })
    }
  }
}

object APIKeyManagerSpec extends APIKeyManagerSpec[Need] {
  val M = Need.need
}

