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

import org.specs2.mutable._

object AccessControlSpec extends Specification with AccessControlTestValues {
  val accessControl = new TokenBasedAccessControl with TestTokenManagerComponent {
    val tokenConfig = accessControlTokenConfig
  }


  "access control" should {
    "control path access" in {
      "allow access" in {
        val accessRoot = accessControl.mayAccessPath(rootUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(rootUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beTrue
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beTrue
        accessChild(PathWrite) must beTrue
      }
      "allow access via grant" in {
        val accessRoot = accessControl.mayAccessPath(grantUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(grantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beTrue
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beTrue
        accessChild(PathWrite) must beTrue
      }
      "limit access to constrained perms" in {
        val accessRoot = accessControl.mayAccessPath(childUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(childUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beTrue
      }
      "limit access via grant" in {
        val accessRoot = accessControl.mayAccessPath(limitedGrantUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(limitedGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beTrue
      }
      "deny access to invalid uid" in {
        val accessRoot = accessControl.mayAccessPath(invalidUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(invalidUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer uid is invalid" in {
        val accessRoot = accessControl.mayAccessPath(invalidChildUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(invalidChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access to invalid grant" in {
        val accessRoot = accessControl.mayAccessPath(invalidGrantUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(invalidGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer is invalid" in {
        val accessRoot = accessControl.mayAccessPath(invalidGrantChildUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(invalidGrantChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access with no perms" in {
        val accessRoot = accessControl.mayAccessPath(noPermsUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(noPermsUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer no longer has perms" in {
        val accessRoot = accessControl.mayAccessPath(noPermsChildUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(noPermsChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer no longer has perms" in {
        val accessRoot = accessControl.mayAccessPath(noPermsGrantUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(noPermsGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer may not share perms" in {
        val accessRoot = accessControl.mayAccessPath(noShareChildUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(noShareChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer may not share perms" in {
        val accessRoot = accessControl.mayAccessPath(noShareGrantUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(noShareGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when expired" in {
        val accessRoot = accessControl.mayAccessPath(expiredUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(expiredUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant expired" in {
        val accessRoot = accessControl.mayAccessPath(grantExpiredUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(grantExpiredUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer expired" in {
        val accessRoot = accessControl.mayAccessPath(expiredChildUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(expiredChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer expired" in {
        val accessRoot = accessControl.mayAccessPath(grantExpiredChildUID, "/", _: PathAccess)
        val accessChild = accessControl.mayAccessPath(grantExpiredChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
    }
    "control data access" in {
      "allow access" in {
        accessControl.mayAccessData(rootUID, "/", Set(rootUID), DataQuery) must beTrue
        accessControl.mayAccessData(rootUID, "/child", Set(rootUID), DataQuery) must beTrue
      }
      "limit access" in {
        accessControl.mayAccessData(childUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(childUID, "/child", Set(rootUID), DataQuery) must beTrue
      }
      "deny access to invalid uid" in {
        accessControl.mayAccessData(invalidUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(invalidUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer uid is invalid" in {
        accessControl.mayAccessData(invalidChildUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(invalidChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access with no perms" in {
        accessControl.mayAccessData(noDataPermsUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(noDataPermsUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer no longer has perms" in {
        accessControl.mayAccessData(noDataPermsChildUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(noDataPermsChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer may not perms" in {
        accessControl.mayAccessData(noDataShareChildUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(noDataShareChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when expired" in {
        accessControl.mayAccessData(expiredUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(expiredUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer expired" in {
        accessControl.mayAccessData(expiredChildUID, "/", Set(rootUID), DataQuery) must beFalse
        accessControl.mayAccessData(expiredChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
    }
  }
}

object AccessControlUseCasesSpec extends Specification with AccessControlTestValues {
 
  val accessControl = new TokenBasedAccessControl with TestTokenManagerComponent {
    val tokenConfig = useCaseTokenConfig
  }

  "access control" should {
    "handle proposed use cases" in {
      "addon grants sandboxed to user paths" in {
        accessControl.mayAccessPath(customerUID, "/customer", PathRead) must beTrue
        accessControl.mayAccessPath(customerUID, "/knownCustomer", PathRead) must beTrue
        accessControl.mayAccessPath(customerUID, "/unknownCustomer", PathRead) must beFalse

        accessControl.mayAccessData(customerUID, "/customer", Set(customerUID), DataQuery) must beTrue
        accessControl.mayAccessData(customerUID, "/customer", Set(customerUID, addonUID), DataQuery) must beTrue
        accessControl.mayAccessData(customerUID, "/knownCustomer", Set(knownCustomerUID), DataQuery) must beTrue
        accessControl.mayAccessData(customerUID, "/knownCustomer", Set(knownCustomerUID, customerUID), DataQuery) must beTrue

        accessControl.mayAccessData(customerUID, "/knownCustomer", Set(knownCustomerUID, customerUID, addonCustomerGrantUID), DataQuery) must beFalse
        accessControl.mayAccessData(customerUID, "/unknownCustomer", Set(unknownCustomerUID), DataQuery) must beFalse
        accessControl.mayAccessData(customerUID, "/unknownCustomer", Set(unknownCustomerUID, addonCustomerGrantUID), DataQuery) must beFalse
        accessControl.mayAccessData(customerUID, "/unknownCustomer", Set(addonCustomerGrantUID), DataQuery) must beFalse
      }
      "addon grants can be passed to our customer's customers" in {
        accessControl.mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID), DataQuery) must beTrue
        accessControl.mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID, addonUID), DataQuery) must beTrue
        accessControl.mayAccessData(customersCustomerUID, "/customer", Set(customerUID), DataQuery) must beFalse
        accessControl.mayAccessData(customersCustomerUID, "/customer", Set(customerUID, addonUID), DataQuery) must beFalse
      }
      "ability to access data created by an agent (child) of the granter" in {
        accessControl.mayAccessData(customerUID, "/customer", Set(customerUID, addonAgentUID), DataQuery) must beTrue
        accessControl.mayAccessData(customersCustomerUID, "/customer", Set(customerUID, addonAgentUID), DataQuery) must beFalse
        accessControl.mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID, addonAgentUID), DataQuery) must beTrue
      }
      "ability to grant revokable public access" in {
        accessControl.mayAccessData(customerUID, "/addon/public", Set(addonUID), DataQuery) must beTrue
      }
    }
  }
}

object AccessControlIsolationSpec extends Specification with AccessControlTestValues {

  val accessControl = new TokenBasedAccessControl with TestTokenManagerComponent {
    val tokenConfig = useCaseTokenConfig
  }

  "access control" should {
    "handle proposed use cases" in {
      accessControl.mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID), DataQuery) must beTrue
      accessControl.mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID, addonUID), DataQuery) must beTrue
      accessControl.mayAccessData(customersCustomerUID, "/customer", Set(customerUID), DataQuery) must beFalse
      accessControl.mayAccessData(customersCustomerUID, "/customer", Set(customerUID, addonUID), DataQuery) must beFalse
    }
  }
}

trait AccessControlTestValues { 

  implicit def stringToPath(path: String) = Path(path)

  val rootUID = "root"
  val childUID = "childPerms"
 
  val grantUID = "grant"
  val grantGrantUID = "grantGrant"
  val limitedGrantUID = "limitedGrant"
  val limitedGrantGrantUID = "limitedGrantGrant"
  
  val invalidUID = "invalid"
  val invalidChildUID = "invalidChild"
  val invalidGrantUID = "invalidGrant"
  val invalidGrantChildUID = "invalidGrantChild"

  val noPermsUID = "noPerms"
  val noPermsChildUID = "noPermsChild"
  val noPermsGrantUID = "noPermsGrant"
  val noDataPermsUID = "noDataPerms"
  val noDataPermsChildUID = "noDataPermsChild"
 
  val expiredUID = "expired"
  val expiredChildUID = "expiredChild"
  val grantExpiredUID = "grantExpired"
  val grantExpiredChildUID = "grantExpiredChild"
  
  val noShareUID = "noShare"
  val noShareChildUID = "noShareChild"
  val noShareGrantUID = "noShareGrant"
  val noDataShareUID = "noDataShare"
  val noDataShareChildUID = "noDataShareChild"
 
  val customerUID = "customer"
  val customersCustomerUID = "customersCustomer"
  val knownCustomerUID = "knownCustomer"
  val knownCustomerGrantUID = "knownCustomerGrant"
  val unknownCustomerUID = "unknownCustomer"
 
  val addonUID = "addon"
  val addonPublicUID = "addonPublic"
  val addonAgentUID = "addonAgent"
  
  val addonCustomerGrantUID = "addonCustomerGrant"
  val addonKnownCustomerGrantUID = "addonKnownCustomerGrant"
  val addonUnknownCustomerGrantUID = "addonUnknownCustomerGrant"
 
  val mayReadRoot = MayAccessPath(Subtree("/"), PathRead, true)
  val mayWriteRoot = MayAccessPath(Subtree("/"), PathWrite, true)
  val mayQueryRoot = MayAccessData(Subtree("/"), OwnerAndDescendants(rootUID), DataQuery, true)

  val mayReadRootNS = MayAccessPath(Subtree("/"), PathRead, false)
  val mayWriteRootNS = MayAccessPath(Subtree("/"), PathWrite, false)
  val mayQueryRootNS = MayAccessData(Subtree("/"), OwnerAndDescendants(rootUID), DataQuery, false)
  
  val mayReadChild = MayAccessPath(Subtree("/child"), PathRead, true)
  val mayWriteChild = MayAccessPath(Subtree("/child"), PathWrite, true)
  val mayQueryChild = MayAccessData(Subtree("/child"), OwnerAndDescendants(rootUID), DataQuery, true)

  def readWritePerms(path: Path, mayShare: Boolean) = 
    Permissions(
      MayAccessPath(Subtree(path), PathRead, mayShare), 
      MayAccessPath(Subtree(path), PathWrite, mayShare)
    )()
  
  def readWriteQueryPerms(path: Path, owner: UID, mayShare: Boolean) = 
    Permissions(
      MayAccessPath(Subtree(path), PathRead, mayShare), 
      MayAccessPath(Subtree(path), PathWrite, mayShare)
    )(
      MayAccessData(Subtree(path), OwnerAndDescendants(owner), DataQuery, mayShare)
    )
  
  def queryPerms(path: Path, owner: UID, mayShare: Boolean) =
    Permissions()(
      MayAccessData(Subtree(path), OwnerAndDescendants(owner), DataQuery, mayShare)
    )
  
  val accessControlTokenConfig = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
    (rootUID, None, Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),
    (childUID, Some(rootUID), Permissions(mayReadChild, mayWriteChild)(mayQueryChild), Set(), false),

    (grantGrantUID, Some(rootUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),
    (grantUID, None, Permissions()(), Set(grantGrantUID), false),
    (limitedGrantGrantUID, Some(rootUID), Permissions(mayReadChild, mayWriteChild)(mayQueryChild), Set(), false),
    (limitedGrantUID, None, Permissions()(), Set(limitedGrantGrantUID), false),
    
    (invalidGrantUID, None, Permissions()(), Set(invalidUID), false),
    (invalidChildUID, Some(invalidUID), Permissions(mayReadRoot, mayWriteRoot)(), Set(), false),
    (invalidGrantChildUID, Some(invalidUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),

    (noPermsUID, None, Permissions()(), Set(), false),
    (noPermsChildUID, Some(noPermsUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),
    (noPermsGrantUID, None, Permissions()(), Set(noPermsChildUID), false),
    (noDataPermsUID, None, Permissions(mayReadRoot, mayWriteRoot)(), Set(), false),
    (noDataPermsChildUID, Some(noDataPermsUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),

    (expiredUID, None, Permissions(mayReadRoot, mayWriteRoot)(), Set(), true),
    (expiredChildUID, Some(expiredUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),
    (grantExpiredUID, None, Permissions()(), Set(expiredUID), false),
    (grantExpiredChildUID, None, Permissions()(), Set(expiredChildUID), false),

    (noShareUID, None, Permissions(mayReadRootNS, mayWriteRootNS)(mayQueryRootNS), Set(), false),
    (noShareChildUID, Some(noShareUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),
    (noShareGrantUID, None, Permissions()(), Set(noShareChildUID), false),
    (noDataShareUID, None, Permissions(mayReadRoot, mayWriteRoot)(mayQueryRootNS), Set(), false),
    (noDataShareChildUID, Some(noShareUID), Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false)
  )
 
  def standardAccountPerms(path: Path, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(path), PathRead, mayShare), 
      MayAccessPath(Subtree(path), PathWrite, mayShare)
    )(
      MayAccessData(Subtree("/"), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  def customerAddonPerms(customerPath: Path, addonOwner: UID, mayShare: Boolean = true) =
    Permissions(
    )(
      MayAccessData(Subtree(customerPath), OwnerAndDescendants(addonOwner), DataQuery, mayShare)
    )

  def publishPathPerms(path: Path, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(path), PathRead, mayShare)
    )(
      MayAccessData(Subtree(path), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  val useCaseTokenConfig = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
    (rootUID, None, Permissions(mayReadRoot, mayWriteRoot)(mayQueryRoot), Set(), false),

    (addonUID, Some(rootUID), standardAccountPerms("/addon", addonUID, true), Set(), false),

    (addonAgentUID, Some(addonUID), readWritePerms("/addon", false), Set(), false),
    
    (addonPublicUID, Some(addonUID), publishPathPerms("/addon/public", addonUID, true), Set(), false),
   
    (addonCustomerGrantUID, Some(addonUID), customerAddonPerms("/customer", addonUID), Set(), false),
    (addonKnownCustomerGrantUID, Some(addonUID), customerAddonPerms("/knownCustomer", addonUID), Set(), false),
    (addonUnknownCustomerGrantUID, Some(addonUID), customerAddonPerms("/unknownCustomer", addonUID), Set(), false),

    (customerUID, Some(rootUID), standardAccountPerms("/customer", customerUID, true), Set(addonCustomerGrantUID, knownCustomerGrantUID, addonPublicUID), false),
    (customersCustomerUID, Some(customerUID), readWriteQueryPerms("/customer/cust-id", customersCustomerUID, false) ++ queryPerms("/customer/cust-id", addonUID, false), Set(), false),
    
    (knownCustomerUID, Some(rootUID), standardAccountPerms("/knownCustomer", knownCustomerUID, true), Set(addonKnownCustomerGrantUID), false),
    (knownCustomerGrantUID, Some(knownCustomerUID), readWriteQueryPerms("/knownCustomer", knownCustomerUID, true), Set(), false),

    (unknownCustomerUID, Some(rootUID), standardAccountPerms("/unknownCustomer", unknownCustomerUID, true), Set(addonUnknownCustomerGrantUID), false)
  )
}

trait TestTokenManagerComponent extends TokenManagerComponent {
  def tokenConfig: List[(UID, Option[UID], Permissions, Set[UID], Boolean)]

  lazy val map = Map( tokenConfig map {
    case (uid, issuer, perms, grants, canShare) => (uid -> Token(uid, issuer, perms, grants, canShare))
  }: _*)

  lazy val tokenManager = new TokenManager {
    def lookup(uid: UID) = map.get(uid)
  }
}
