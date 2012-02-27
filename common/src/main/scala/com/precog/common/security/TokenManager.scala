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
package com.precog.common.security

import com.precog.analytics.Path

trait TokenManagerComponent {
  def tokenManager: TokenManager
}

trait TokenManager {
  def lookup(uid: UID): Option[Token]
}

object StaticTokenManager extends TokenManager {

  val rootUID = "C18ED787-BF07-4097-B819-0415C759C8D5"
  
  val testUID = "03C4F5FE-69E2-4151-9D93-7C986936CB86"
  val usageUID = "6EF2E81E-D9E8-4DC6-AD66-DEB30A164F73"
  
  val publicUID = "B88E82F0-B78B-49D9-A36B-78E0E61C4EDC"
 
  val cust1UID = "C5EF0038-A2A2-47EB-88A4-AAFCE59EC22B"
  val cust2UID = "1B10E413-FB5B-4769-A887-8AFB587CF00A"
  
  def standardAccountPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare), 
      MayAccessPath(Subtree(Path(path)), PathWrite, mayShare)
    )(
      MayAccessData(Subtree(Path("/")), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  def publishPathPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare)
    )(
      MayAccessData(Subtree(Path(path)), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  private val config = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
    (rootUID, None, standardAccountPerms("/", rootUID, true), Set(), false),
    (publicUID, Some(rootUID), publishPathPerms("/public", rootUID, true), Set(), false),
    (testUID, Some(rootUID), standardAccountPerms("/unittest", testUID, true), Set(), false),
    (usageUID, Some(rootUID), standardAccountPerms("/__usage_tracking__", usageUID, true), Set(), false),
    (cust1UID, Some(rootUID), standardAccountPerms("/user1", cust1UID, true), Set(publicUID), false),
    (cust2UID, Some(rootUID), standardAccountPerms("/user2", cust2UID, true), Set(publicUID), false)
  )

  private lazy val map = Map( config map {
    case (uid, issuer, perms, grants, canShare) => (uid -> Token(uid, issuer, perms, grants, canShare))
  }: _*)
  
  def lookup(uid: UID) = map.get(uid)

}
