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

import akka.util.Duration
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.ExecutionContext

import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo.{Mongo, RealMongoSpecSupport}

import scala.collection

import org.joda.time.DateTime

import scalaz._

class AccessControlSpec extends Specification with RealMongoSpecSupport with APIKeyManagerTestValues with AccessControlHelpers {
  lazy val state = new APIKeyManagerTestValuesState
  import state._

  implicit lazy val accessControl = new APIKeyManagerAccessControl(apiKeys)
  implicit lazy val M: Monad[Future] = blueeyes.bkka.AkkaTypeClasses.futureApplicative(defaultFutureDispatch)

  "access control" should {
    
    "control path access" in {
      "allow access" in {
        val accessRoot = mayAccess(rootAPIKey.tid, "/", Set(rootAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(rootAPIKey.tid, "/child", Set(rootAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beTrue
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beTrue
        accessChild(WritePermission) must beTrue
      }
      "limit access" in {
        val accessRoot = mayAccess(rootLikeAPIKey.tid, "/", Set(rootLikeAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(rootLikeAPIKey.tid, "/child", Set(rootLikeAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beTrue
      }
      "allow access via grant" in {
        val accessRoot = mayAccess(superAPIKey.tid, "/", Set(rootAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(superAPIKey.tid, "/child", Set(rootAPIKey.tid), _: AccessType)

        accessRoot(ReadPermission) must beTrue
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beTrue
        accessChild(WritePermission) must beTrue
      }
      "limit access via grant" in {
        val accessRoot = mayAccess(childAPIKey.tid, "/", Set(childAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(childAPIKey.tid, "/child", Set(childAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beTrue
      }
      "deny access to invalid uid" in {
        val invalidUID = "not-there"
        val accessRoot = mayAccess(invalidUID, "/", Set(invalidUID), _: AccessType)
        val accessChild = mayAccess(invalidUID, "/child", Set(invalidUID), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access to invalid grant" in {
        val accessRoot = mayAccess(invalidGrantAPIKey.tid, "/", Set(invalidGrantAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(invalidGrantAPIKey.tid, "/child", Set(invalidGrantAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access when grant issuer is invalid" in {
        val accessRoot = mayAccess(invalidGrantParentAPIKey.tid, "/", Set(invalidGrantParentAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(invalidGrantParentAPIKey.tid, "/child", Set(invalidGrantParentAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access with no grants" in {
        val accessRoot = mayAccess(noPermsAPIKey.tid, "/", Set(noPermsAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(noPermsAPIKey.tid, "/child", Set(noPermsAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access when grant expired" in {
        val accessRoot = mayAccess(expiredAPIKey.tid, "/", Set(expiredAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(expiredAPIKey.tid, "/child", Set(expiredAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access when grant parent expired" in {
        val accessRoot = mayAccess(expiredParentAPIKey.tid, "/", Set(expiredParentAPIKey.tid), _: AccessType)
        val accessChild = mayAccess(expiredParentAPIKey.tid, "/child", Set(expiredParentAPIKey.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
    }
    
    "control data access" in {
      "allow access" in {
        mayAccess(rootAPIKey.tid, "/", Set(rootAPIKey.tid), ReducePermission) must beTrue
        mayAccess(rootAPIKey.tid, "/child", Set(rootAPIKey.tid), ReducePermission) must beTrue
      }
      "limit access" in {
        mayAccess(childAPIKey.tid, "/", Set(childAPIKey.tid), ReducePermission) must beFalse
        mayAccess(childAPIKey.tid, "/child", Set(childAPIKey.tid), ReducePermission) must beTrue
      }
      "deny access to invalid uid" in {
        mayAccess(invalidUID, "/", Set(invalidUID), ReducePermission) must beFalse
        mayAccess(invalidUID, "/child", Set(invalidUID), ReducePermission) must beFalse
      }
      "deny access to invalid grant" in {
        mayAccess(invalidGrantAPIKey.tid, "/", Set(invalidGrantAPIKey.tid), ReducePermission) must beFalse
        mayAccess(invalidGrantAPIKey.tid, "/child", Set(invalidGrantAPIKey.tid), ReducePermission) must beFalse
      }
      "deny access when parent grant invalid" in {
        mayAccess(invalidGrantParentAPIKey.tid, "/", Set(invalidGrantParentAPIKey.tid), ReducePermission) must beFalse
        mayAccess(invalidGrantParentAPIKey.tid, "/child", Set(invalidGrantParentAPIKey.tid), ReducePermission) must beFalse
      }
      "deny access with no perms" in {
        mayAccess(noPermsAPIKey.tid, "/", Set(noPermsAPIKey.tid), ReducePermission) must beFalse
        mayAccess(noPermsAPIKey.tid, "/child", Set(noPermsAPIKey.tid), ReducePermission) must beFalse
      }
      "deny access when expired" in {
        mayAccess(expiredAPIKey.tid, "/", Set(expiredAPIKey.tid), ReducePermission) must beFalse
        mayAccess(expiredAPIKey.tid, "/child", Set(expiredAPIKey.tid), ReducePermission) must beFalse
      }
      "deny access when issuer expired" in {
        mayAccess(expiredParentAPIKey.tid, "/", Set(expiredParentAPIKey.tid), ReducePermission) must beFalse
        mayAccess(expiredParentAPIKey.tid, "/child", Set(expiredParentAPIKey.tid), ReducePermission) must beFalse
      }
    }
  }
}

class AccessControlUseCasesSpec extends Specification with RealMongoSpecSupport with UseCasesAPIKeyManagerTestValues with AccessControlHelpers {
  lazy val state = new UseCasesAPIKeyManagerTestValuesState
  import state._

  implicit lazy val accessControl = new APIKeyManagerAccessControl(apiKeys)
  implicit lazy val M: Monad[Future] = blueeyes.bkka.AkkaTypeClasses.futureApplicative(defaultFutureDispatch)

  "access control" should {
    "handle proposed use cases" in {
      "addon grants sandboxed to user paths" in {
        mayAccess(customer.tid, "/customer", Set(customer.tid), ReadPermission) must beTrue
        mayAccess(customer.tid, "/customer", Set(customer.tid), ReadPermission) must beTrue
        mayAccess(customer.tid, "/customer", Set(addon.tid), ReadPermission) must beTrue
        mayAccess(customer.tid, "/customer", Set(customer.tid, addon.tid), ReadPermission) must beTrue

        mayAccess(customer.tid, "/friend", Set(friend.tid), ReadPermission) must beTrue
        mayAccess(customer.tid, "/friend", Set(friend.tid, customer.tid), ReadPermission) must beTrue

        //mayAccess(customer.tid, "/friend", Set(addon.tid), ReadPermission) must beFalse
        //mayAccess(customer.tid, "/friend", Set(friend.tid, customer.tid, addon.tid), ReadPermission) must beFalse
        
        mayAccess(customer.tid, "/stranger", Set(stranger.tid), ReadPermission) must beFalse
        mayAccess(customer.tid, "/stranger", Set(addon.tid), ReadPermission) must beFalse
        mayAccess(customer.tid, "/stranger", Set(stranger.tid, addon.tid), ReadPermission) must beFalse
      }
      "addon grants can be passed to our customer's customers" in {
        mayAccess(customersCustomer.tid, "/customer/cust-id", Set(customersCustomer.tid), ReadPermission) must beTrue
        mayAccess(customersCustomer.tid, "/customer/cust-id", Set(customersCustomer.tid, addon.tid), ReadPermission) must beTrue
        mayAccess(customersCustomer.tid, "/customer", Set(customer.tid), ReadPermission) must beFalse
        mayAccess(customersCustomer.tid, "/customer", Set(customer.tid, addon.tid), ReadPermission) must beFalse
      }
      "ability to access data created by an agent (child) of the granter" in {
        mayAccess(customer.tid, "/customer", Set(customer.tid, addonAgent.tid), ReadPermission) must beTrue
        mayAccess(customersCustomer.tid, "/customer", Set(customer.tid, addonAgent.tid), ReadPermission) must beFalse
        mayAccess(customersCustomer.tid, "/customer/cust-id", Set(customersCustomer.tid, addonAgent.tid), ReadPermission) must beTrue
      }
      "ability to grant revokable public access" in {
        mayAccess(customer.tid, "/addon/public", Set(addon.tid), ReadPermission) must beTrue
        mayAccess(customer.tid, "/addon/public_revoked", Set(addon.tid), ReadPermission) must beFalse
      }
    }
  }
}

trait AccessControlHelpers {

  val testTimeout = Duration(30, "seconds")

  def mayAccess(uid: UID, path: Path, owners: Set[UID], accessType: AccessType)(implicit ac: AccessControl[Future]): Boolean = {
    Await.result(ac.mayAccess(uid, path, owners, accessType), testTimeout)
  }
}

trait APIKeyManagerTestValues extends AkkaDefaults { self : Specification =>
  val invalidUID = "invalid"
  val timeout = Duration(30, "seconds")

  def mongo: Mongo

  implicit def stringToPath(path: String): Path = Path(path)
  
  val farFuture = new DateTime().plusYears(1000)
  val farPast = new DateTime().minusYears(1000)

  class APIKeyManagerTestValuesState {
    val database = mongo.database("apikey_test_database")

    val apiKeys = new MongoAPIKeyManager(mongo, database)

    def newAPIKey(name: String)(f: APIKeyRecord => Set[GrantID]): (APIKeyRecord, Set[GrantID]) = {
      try {
        val apiKey = Await.result(apiKeys.newAPIKey(name, "", Set.empty), timeout)
        val grants = f(apiKey)
                      (Await.result(apiKeys.addGrants(apiKey.tid, grants), timeout).get, grants)
      } catch {
        case ex => ex.printStackTrace; throw ex
      }
    }
    
    val (rootAPIKey, rootGrants) = newAPIKey("root") { t =>
      try {
        Await.result(Future.sequence(Permission.permissions("/", t.tid, None, Permission.ALL).map{ apiKeys.newGrant(None, _) }.map{ _.map { _.gid } }), timeout)
      } catch {
        case ex => println(ex); throw ex
      }
    }
    
    val (rootLikeAPIKey, rootLikeGrants) = newAPIKey("rootLike") { t =>
      Await.result(Future.sequence(Permission.permissions("/child", t.tid, None, Permission.ALL).map{ apiKeys.newGrant(None, _) }.map{ _.map { _.gid }}), timeout)
    }
    
    val (superAPIKey, superGrants) = newAPIKey("super") { t =>
      Await.result(Future.sequence(rootGrants.map{ g =>
        apiKeys.findGrant(g).flatMap {
          case Some(g) => 
            apiKeys.newGrant(Some(g.gid), g.permission)
          case _ => failure("Grant not found")
        }.map { _.gid }
      }),timeout)
    }

    val (childAPIKey, childGrants) = newAPIKey("child") { t =>
      Await.result(Future.sequence(rootGrants.map{ g =>
        apiKeys.findGrant(g).flatMap { 
          case Some(g) =>
            apiKeys.newGrant(Some(g.gid), g match {
              case Grant(_, _, oi: OwnerIgnorantPermission) => 
                oi.derive(path = "/child")
              case Grant(_, _, oa: OwnerAwarePermission) => 
                oa.derive(path = "/child", owner = t.tid)
            }).map { _.gid }
          case _ => failure("Grant not found")
        }
      }), timeout)
    }

    val invalidGrantID = "not going to find it"

    val (invalidGrantAPIKey, invalidGrantGrants) = newAPIKey("invalidGrant") { t =>
      0.until(6).map { invalidGrantID + _ }(collection.breakOut)
    }

    val (invalidGrantParentAPIKey, invalidGrantParentGrants) = newAPIKey("invalidGrantParent") { t =>
      Await.result(Future.sequence(Permission.permissions("/", t.tid, None, Permission.ALL).map{ apiKeys.newGrant(Some(invalidGrantID), _) }.map{ _.map { _.gid } }), timeout)
    }

    val noPermsAPIKey = Await.result(apiKeys.newAPIKey("noPerms", "", Set.empty), timeout)

    val (expiredAPIKey, expiredGrants) = newAPIKey("expiredGrants") { t =>
      Await.result(Future.sequence(Permission.permissions("/", t.tid, Some(farPast), Permission.ALL).map{ apiKeys.newGrant(None, _) }.map{ _.map { _.gid }}), timeout)
    }

    val (expiredParentAPIKey, expiredParentAPIKeys) = newAPIKey("expiredParentGrants") { t =>
      Await.result(Future.sequence(expiredGrants.map { g =>
        apiKeys.findGrant(g).flatMap { 
          case Some(g) =>
            apiKeys.newGrant(Some(g.gid), g match {
              case Grant(_, _, oi: OwnerIgnorantPermission) => 
                oi.derive(path = "/child")
              case Grant(_, _, oa: OwnerAwarePermission) => 
                oa.derive(path = "/child", owner = t.tid)
            }).map { _.gid }
          case _ => failure("Grant not found")
        }
      }), timeout)
    }
  }
}

trait UseCasesAPIKeyManagerTestValues extends AkkaDefaults { self : Specification =>
  val timeout = Duration(30, "seconds")

  def mongo: Mongo
  
  implicit def stringToPath(path: String): Path = Path(path)

  val farFuture = new DateTime().plusYears(1000)
  val farPast = new DateTime().minusYears(1000)

  class UseCasesAPIKeyManagerTestValuesState {
    val database = mongo.database("use_case_test_database")

    val apiKeys = new MongoAPIKeyManager(mongo, database)

    def newAPIKey(name: String)(f: APIKeyRecord => Set[GrantID]): (APIKeyRecord, Set[GrantID]) = {
      Await.result(apiKeys.newAPIKey(name, "", Set.empty).flatMap { t =>
        val g = f(t)
        apiKeys.addGrants(t.tid, g).map { t => (t.get, g) } 
      }, timeout)
    }
    
    def newCustomer(name: String, parentGrants: Set[GrantID]): (APIKeyRecord, Set[GrantID]) = {
      newAPIKey(name) { t =>
        Await.result(Future.sequence(parentGrants.map{ g =>
          apiKeys.findGrant(g).flatMap { 
            case Some(g) => apiKeys.newGrant(Some(g.gid), g match {
              case Grant(gid, _, oi: OwnerIgnorantPermission) => 
                oi.derive(path = "/" + name)
              case Grant(gid, _, oa: OwnerAwarePermission) => 
                oa.derive(path = "/" + name, owner = t.tid)
            }).map { _.gid }
            case _ => failure("Grant not found")
          }
        }), timeout)
      }
    }

    def addGrants(apiKey: APIKeyRecord, grants: Set[GrantID]): Option[APIKeyRecord] = {
      Await.result(apiKeys.findAPIKey(apiKey.tid).flatMap { _ match {
        case None => Future(None)
        case Some(t) => apiKeys.addGrants(t.tid, grants)
      }}, timeout)
    }

    val (root, rootGrants) = newAPIKey("root") { t =>
      Await.result(Future.sequence(Permission.permissions("/", t.tid, None, Permission.ALL).map{ apiKeys.newGrant(None, _) }.map{ _.map { _.gid } }), timeout)
    }

    val (customer, customerGrants) = newCustomer("customer", rootGrants)
    val (friend, friendGrants) = newCustomer("friend", rootGrants)
    val (stranger, strangerGrants) = newCustomer("stranger", rootGrants)
    val (addon, addonGrants) = newCustomer("addon", rootGrants)

    val (addonAgent, addonAgentGrants) = newAPIKey("addon_agent") { t =>
      Await.result(Future.sequence(addonGrants.map { g =>
        apiKeys.findGrant(g).flatMap { 
          case Some(g) => apiKeys.newGrant(Some(g.gid), g match {
            case Grant(gid, _, oi: OwnerIgnorantPermission) => 
              oi
            case Grant(gid, _, oa: OwnerAwarePermission) => 
              oa.derive(owner = t.tid)
          }).map { _.gid }
          case _ => failure("Grant not found")
        }
      }), timeout)
    }

    val customerFriendGrants: Set[GrantID] = Await.result(Future.sequence(friendGrants.map { gid =>
      apiKeys.findGrant(gid).map { _.flatMap {
        case Grant(_, _, rg @ ReadPermission(_, _, _)) =>
          Some((Some(gid), rg))
        case _ => None
      }}
    }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map{ _.gid } } ) }, timeout)
    
    val customerAddonGrants: Set[GrantID] = Await.result(Future.sequence(addonGrants.map { gid =>
      apiKeys.findGrant(gid).map { _.flatMap { 
        case Grant(_, _, rg @ ReadPermission(_, _, _)) =>
          Some(Some(gid), rg.derive(path = "/customer"))
        case _ => None
      }}
    }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)
    
    val customerAddonAgentGrants: Set[GrantID] = Await.result(Future.sequence(addonAgentGrants.map { gid =>
      apiKeys.findGrant(gid).map { _.map {
        case Grant(_, _, oi: OwnerIgnorantPermission) => 
          (Some(gid), oi.derive(path = "/customer"))
        case Grant(_, _, oa: OwnerAwarePermission) => 
          (Some(gid), oa.derive(path = "/customer"))
      }}
    }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

    val customerAddonPublicGrants: Set[GrantID] = Await.result(Future.sequence(addonGrants.map { gid =>
      apiKeys.findGrant(gid).map { _.flatMap {
        case Grant(_, _, rg @ ReadPermission(_, _, _)) => 
          Some((Some(gid), rg.derive(path = "/addon/public")))
        case _ => None
      }}
    }).flatMap { og => Future.sequence[GrantID, Set](og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

    val customerAddonPublicRevokedGrants: Set[GrantID] = Await.result(Future.sequence(addonGrants.map { gid =>
      apiKeys.findGrant(gid).map { _.flatMap {
        case Grant(_, _, rg @ ReadPermission(_, _, _)) => 
          Some((Some(gid), rg.derive(path = "/addon/public_revoked", expiration = Some(farPast))))
        case _ => None
      }}
    }).flatMap { og => Future.sequence[GrantID, Set](og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

    apiKeys.addGrants(customer.tid, 
                      customerFriendGrants ++ 
                      customerAddonGrants ++ 
                      customerAddonAgentGrants ++
                      customerAddonPublicGrants ++
                      customerAddonPublicRevokedGrants)

    val (customersCustomer, customersCustomerGrants) = newAPIKey("customers_customer") { t =>
      Await.result(apiKeys.findAPIKey(customer.tid).flatMap { ot => Future.sequence(ot.get.grants.map { g =>
        apiKeys.findGrant(g).flatMap { 
          case Some(g) => apiKeys.newGrant(Some(g.gid), g match {
            case Grant(_, _, oi: OwnerIgnorantPermission) => 
              oi.derive(path = "/customer/cust-id")
            case Grant(_, _, oa: OwnerAwarePermission) => 
              oa.derive(path = "/customer/cust-id", owner = t.tid)
          }).map { _.gid }
          case _ => failure("Grant not found")
        }})}, timeout)
    }

    val customersCustomerAddonsGrants: Set[GrantID] = Await.result(apiKeys.findAPIKey(customer.tid).flatMap { ot => Future.sequence(ot.get.grants.map { gid =>
      apiKeys.findGrant(gid).map { _.map {
        case Grant(_, _, oi: OwnerIgnorantPermission) => 
          (Some(gid), oi.derive(path = "/customer/cust-id"))
        case Grant(_, _, oa: OwnerAwarePermission) => 
          (Some(gid), oa.derive(path = "/customer/cust-id"))
      }}
    }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map { _.gid } } ) }}, timeout)
    
    val customersCustomerAddonAgentGrants: Set[GrantID] = Await.result(Future.sequence(addonAgentGrants.map { gid =>
      apiKeys.findGrant(gid).map { _.map { 
        case Grant(_, _, oi: OwnerIgnorantPermission) => 
          (Some(gid), oi.derive(path = "/customer/cust-id"))
        case Grant(_, _, oa: OwnerAwarePermission) => 
          (Some(gid), oa.derive(path = "/customer/cust-id"))
      }}
    }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => apiKeys.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

    apiKeys.addGrants(customersCustomer.tid, customersCustomerAddonsGrants ++ customersCustomerAddonAgentGrants)
  }
}
