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

import accounts.AccountId
import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

trait APIKeyFinder[M[+_]] extends AccessControl[M] with Logging {
  implicit def M : Monad[M]

  def findAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def findGrant(gid: GrantId): M[Option[Grant]]

  def isValidGrant(grantId: GrantId, at: Option[DateTime] = None): M[Option[Grant]] =
    findGrant(grantId).flatMap { grantOpt =>
      grantOpt.map { grant =>
        if(grant.isExpired(at)) None.point[M]
        else grant.parentIds.foldLeft(some(grant).point[M]) { case (accM, parentId) =>
          accM.flatMap(_.map { grant => isValidGrant(parentId, at).map(_ => grant) }.sequence)
        }
      }.getOrElse(None.point[M])
    }
  
  def validGrants(apiKey: APIKey, at: Option[DateTime] = None): M[Set[Grant]] = {
    logger.trace("Checking grant validity for apiKey " + apiKey)
    findAPIKey(apiKey).flatMap(_.map { apiKeyRecord =>
      apiKeyRecord.grants.map(isValidGrant(_, at)).sequence.map(_.flatten)
    }.getOrElse(Set.empty.point[M]))
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime] = None): M[Boolean] =
    validGrants(apiKey, at).map(Grant.implies(_, perms, at))
}
 
