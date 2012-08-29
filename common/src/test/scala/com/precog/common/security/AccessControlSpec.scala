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
import blueeyes.persistence.mongo.mock.MockMongo

import scala.collection

import org.joda.time.DateTime

import scalaz._

class AccessControlSpec extends Specification with TokenManagerTestValues with AccessControlHelpers {

  implicit lazy val accessControl = new TokenManagerAccessControl(tokens)
  implicit lazy val M: Monad[Future] = blueeyes.bkka.AkkaTypeClasses.futureApplicative(defaultFutureDispatch)

  "legacy access control" should {
    "control path access" in {
      "allow access" in {
        val accessRoot = mayAccess(rootToken.tid, "/", Set(rootToken.tid), _: AccessType)
        val accessChild = mayAccess(rootToken.tid, "/child", Set(rootToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beTrue
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beTrue
        accessChild(WritePermission) must beTrue
      }
      "limit access" in {
        val accessRoot = mayAccess(rootLikeToken.tid, "/", Set(rootLikeToken.tid), _: AccessType)
        val accessChild = mayAccess(rootLikeToken.tid, "/child", Set(rootLikeToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beTrue
      }
      "allow access via grant" in {
        val accessRoot = mayAccess(superToken.tid, "/", Set(rootToken.tid), _: AccessType)
        val accessChild = mayAccess(superToken.tid, "/child", Set(rootToken.tid), _: AccessType)

        accessRoot(ReadPermission) must beTrue
        accessChild(ReadPermission) must beTrue
        accessRoot(WritePermission) must beTrue
        accessChild(WritePermission) must beTrue
      }
      "limit access via grant" in {
        val accessRoot = mayAccess(childToken.tid, "/", Set(childToken.tid), _: AccessType)
        val accessChild = mayAccess(childToken.tid, "/child", Set(childToken.tid), _: AccessType)
      
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
        val accessRoot = mayAccess(invalidGrantToken.tid, "/", Set(invalidGrantToken.tid), _: AccessType)
        val accessChild = mayAccess(invalidGrantToken.tid, "/child", Set(invalidGrantToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access when grant issuer is invalid" in {
        val accessRoot = mayAccess(invalidGrantParentToken.tid, "/", Set(invalidGrantParentToken.tid), _: AccessType)
        val accessChild = mayAccess(invalidGrantParentToken.tid, "/child", Set(invalidGrantParentToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access with no grants" in {
        val accessRoot = mayAccess(noPermsToken.tid, "/", Set(noPermsToken.tid), _: AccessType)
        val accessChild = mayAccess(noPermsToken.tid, "/child", Set(noPermsToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access when grant expired" in {
        val accessRoot = mayAccess(expiredToken.tid, "/", Set(expiredToken.tid), _: AccessType)
        val accessChild = mayAccess(expiredToken.tid, "/child", Set(expiredToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
      "deny access when grant parent expired" in {
        val accessRoot = mayAccess(expiredParentToken.tid, "/", Set(expiredParentToken.tid), _: AccessType)
        val accessChild = mayAccess(expiredParentToken.tid, "/child", Set(expiredParentToken.tid), _: AccessType)
      
        accessRoot(ReadPermission) must beFalse
        accessChild(ReadPermission) must beFalse
        accessRoot(WritePermission) must beFalse
        accessChild(WritePermission) must beFalse
      }
    }
    "control data access" in {
      "allow access" in {
        mayAccess(rootToken.tid, "/", Set(rootToken.tid), ReducePermission) must beTrue
        mayAccess(rootToken.tid, "/child", Set(rootToken.tid), ReducePermission) must beTrue
      }
      "limit access" in {
        mayAccess(childToken.tid, "/", Set(childToken.tid), ReducePermission) must beFalse
        mayAccess(childToken.tid, "/child", Set(childToken.tid), ReducePermission) must beTrue
      }
      "deny access to invalid uid" in {
        mayAccess(invalidUID, "/", Set(invalidUID), ReducePermission) must beFalse
        mayAccess(invalidUID, "/child", Set(invalidUID), ReducePermission) must beFalse
      }
      "deny access to invalid grant" in {
        mayAccess(invalidGrantToken.tid, "/", Set(invalidGrantToken.tid), ReducePermission) must beFalse
        mayAccess(invalidGrantToken.tid, "/child", Set(invalidGrantToken.tid), ReducePermission) must beFalse
      }
      "deny access when parent grant invalid" in {
        mayAccess(invalidGrantParentToken.tid, "/", Set(invalidGrantParentToken.tid), ReducePermission) must beFalse
        mayAccess(invalidGrantParentToken.tid, "/child", Set(invalidGrantParentToken.tid), ReducePermission) must beFalse
      }
      "deny access with no perms" in {
        mayAccess(noPermsToken.tid, "/", Set(noPermsToken.tid), ReducePermission) must beFalse
        mayAccess(noPermsToken.tid, "/child", Set(noPermsToken.tid), ReducePermission) must beFalse
      }
      "deny access when expired" in {
        mayAccess(expiredToken.tid, "/", Set(expiredToken.tid), ReducePermission) must beFalse
        mayAccess(expiredToken.tid, "/child", Set(expiredToken.tid), ReducePermission) must beFalse
      }
      "deny access when issuer expired" in {
        mayAccess(expiredParentToken.tid, "/", Set(expiredParentToken.tid), ReducePermission) must beFalse
        mayAccess(expiredParentToken.tid, "/child", Set(expiredParentToken.tid), ReducePermission) must beFalse
      }
    }
  }
}

class AccessControlUseCasesSpec extends Specification with UseCasesTokenManagerTestValues with AccessControlHelpers {
 
  implicit lazy val accessControl = new TokenManagerAccessControl(tokens)
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

        mayAccess(customer.tid, "/friend", Set(addon.tid), ReadPermission) must beFalse
        mayAccess(customer.tid, "/friend", Set(friend.tid, customer.tid, addon.tid), ReadPermission) must beFalse
        
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
        mayAccess(customer.tid, "/addon/revoked_public", Set(addon.tid), ReadPermission) must beFalse
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

trait TokenManagerTestValues extends AkkaDefaults { self : Specification =>

  val invalidUID = "invalid"
  val timeout = Duration(30, "seconds")

  implicit def stringToPath(path: String): Path = Path(path)
  
  val farFuture = new DateTime().plusYears(1000)
  val farPast = new DateTime().minusYears(1000)

  val mongo = new MockMongo
  val database = mongo.database("test_database")

  val tokens = new MongoTokenManager(mongo, database)


  def newToken(name: String)(f: Token => Set[GrantID]): (Token, Set[GrantID]) = {
    try {
      val token = Await.result(tokens.newToken(name, Set.empty), timeout)
      val grants = f(token)
      (Await.result(tokens.addGrants(token.tid, grants), timeout).get, grants)
    } catch {
      case ex => ex.printStackTrace; throw ex
    }
  }
  
  val (rootToken, rootGrants) = newToken("root") { t =>
    try {
    Await.result(Future.sequence(Permission.permissions("/", t.tid, None, Permission.ALL).map{ tokens.newGrant(None, _) }.map{ _.map { _.gid } }), timeout)
    } catch {
      case ex => println(ex); throw ex
    }
  }
  
  val (rootLikeToken, rootLikeGrants) = newToken("rootLike") { t =>
    Await.result(Future.sequence(Permission.permissions("/child", t.tid, None, Permission.ALL).map{ tokens.newGrant(None, _) }.map{ _.map { _.gid }}), timeout)
  }
 
  val (superToken, superGrants) = newToken("super") { t =>
    Await.result(Future.sequence(rootGrants.map{ g =>
      tokens.findGrant(g).flatMap {
        case Some(g) => 
          tokens.newGrant(Some(g.gid), g.permission)
        case _ => failure("Grant not found")
      }.map { _.gid }
    }),timeout)
  }

  val (childToken, childGrants) = newToken("child") { t =>
    Await.result(Future.sequence(rootGrants.map{ g =>
      tokens.findGrant(g).flatMap { 
        case Some(g) =>
          tokens.newGrant(Some(g.gid), g match {
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

  val (invalidGrantToken, invalidGrantGrants) = newToken("invalidGrant") { t =>
    0.until(6).map { invalidGrantID + _ }(collection.breakOut)
  }

  val (invalidGrantParentToken, invalidGrantParentGrants) = newToken("invalidGrantParent") { t =>
    Await.result(Future.sequence(Permission.permissions("/", t.tid, None, Permission.ALL).map{ tokens.newGrant(Some(invalidGrantID), _) }.map{ _.map { _.gid } }), timeout)
  }

  val noPermsToken = Await.result(tokens.newToken("noPerms", Set.empty), timeout)

  val (expiredToken, expiredGrants) = newToken("expiredGrants") { t =>
    Await.result(Future.sequence(Permission.permissions("/", t.tid, Some(farPast), Permission.ALL).map{ tokens.newGrant(None, _) }.map{ _.map { _.gid }}), timeout)
  }

  val (expiredParentToken, expiredParentTokens) = newToken("expiredParentGrants") { t =>
    Await.result(Future.sequence(expiredGrants.map { g =>
      tokens.findGrant(g).flatMap { 
        case Some(g) =>
          tokens.newGrant(Some(g.gid), g match {
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

trait UseCasesTokenManagerTestValues extends AkkaDefaults { self : Specification =>

  val timeout = Duration(30, "seconds")
  
  implicit def stringToPath(path: String): Path = Path(path)

  val farFuture = new DateTime().plusYears(1000)
  val farPast = new DateTime().minusYears(1000)

  val mongo = new MockMongo
  val database = mongo.database("test_database")

  val tokens = new MongoTokenManager(mongo, database)

  def newToken(name: String)(f: Token => Set[GrantID]): (Token, Set[GrantID]) = {
    Await.result(tokens.newToken(name, Set.empty).flatMap { t =>
      val g = f(t)
      tokens.addGrants(t.tid, g).map { t => (t.get, g) } 
    }, timeout)
  }
  
  def newCustomer(name: String, parentGrants: Set[GrantID]): (Token, Set[GrantID]) = {
    newToken(name) { t =>
      Await.result(Future.sequence(parentGrants.map{ g =>
        tokens.findGrant(g).flatMap { 
          case Some(g) => tokens.newGrant(Some(g.gid), g match {
            case Grant(gid, _, oi: OwnerIgnorantPermission) => 
              oi.derive(path = "/" + name)
            case Grant(gid, _, oa: OwnerAwarePermission) => 
              oa.derive(path = "/", owner = t.tid)
          }).map { _.gid }
          case _ => failure("Grant not found")
        }
      }), timeout)
    }
  }

  def addGrants(token: Token, grants: Set[GrantID]): Option[Token] = {
    Await.result(tokens.findToken(token.tid).flatMap { _ match {
      case None => Future(None)
      case Some(t) => tokens.addGrants(t.tid, grants)
    }}, timeout)
  }

  val (root, rootGrants) = newToken("root") { t =>
    Await.result(Future.sequence(Permission.permissions("/", t.tid, None, Permission.ALL).map{ tokens.newGrant(None, _) }.map{ _.map { _.gid } }), timeout)
  }

  val (customer, customerGrants) = newCustomer("customer", rootGrants)
  val (friend, friendGrants) = newCustomer("friend", rootGrants)
  val (stranger, strangerGrants) = newCustomer("stranger", rootGrants)
  val (addon, addonGrants) = newCustomer("addon", rootGrants)

  val (addonAgent, addonAgentGrants) = newToken("addon_agent") { t =>
    Await.result(Future.sequence(addonGrants.map { g =>
      tokens.findGrant(g).flatMap { 
        case Some(g) => tokens.newGrant(Some(g.gid), g match {
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
    tokens.findGrant(gid).map { _.flatMap {
      case Grant(_, _, rg @ ReadPermission(_, _, _)) =>
        Some((Some(gid), rg))
      case _ => None
    }}
  }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map{ _.gid } } ) }, timeout)

  val customerAddonGrants: Set[GrantID] = Await.result(Future.sequence(addonGrants.map { gid =>
    tokens.findGrant(gid).map { _.flatMap { 
      case Grant(_, _, rg @ ReadPermission(_, _, _)) =>
        Some(Some(gid), rg.derive(path = "/customer"))
      case _ => None
    }}
  }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)
  
  val customerAddonAgentGrants: Set[GrantID] = Await.result(Future.sequence(addonAgentGrants.map { gid =>
    tokens.findGrant(gid).map { _.map {
      case Grant(_, _, oi: OwnerIgnorantPermission) => 
        (Some(gid), oi.derive(path = "/customer"))
      case Grant(_, _, oa: OwnerAwarePermission) => 
        (Some(gid), oa.derive(path = "/customer"))
    }}
  }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

  val customerAddonPublicGrants: Set[GrantID] = Await.result(Future.sequence(addonGrants.map { gid =>
    tokens.findGrant(gid).map { _.flatMap {
      case Grant(_, _, rg @ ReadPermission(_, _, _)) => 
        Some((Some(gid), rg.derive(path = "/addon/public")))
      case _ => None
    }}
  }).flatMap { og => Future.sequence[GrantID, Set](og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

  val customerAddonPublicRevokedGrants: Set[GrantID] = Await.result(Future.sequence(addonGrants.map { gid =>
    tokens.findGrant(gid).map { _.flatMap {
      case Grant(_, _, rg @ ReadPermission(_, _, _)) => 
        Some((Some(gid), rg.derive(path = "/addon/public_revoked", expiration = Some(farPast))))
      case _ => None
    }}
  }).flatMap { og => Future.sequence[GrantID, Set](og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

  tokens.addGrants(customer.tid, 
    customerFriendGrants ++ 
    customerAddonGrants ++ 
    customerAddonAgentGrants ++
    customerAddonPublicGrants ++
    customerAddonPublicRevokedGrants)

  val (customersCustomer, customersCustomerGrants) = newToken("customers_customer") { t =>
    Await.result(tokens.findToken(customer.tid).flatMap { ot => Future.sequence(ot.get.grants.map { g =>
      tokens.findGrant(g).flatMap { 
      case Some(g) => tokens.newGrant(Some(g.gid), g match {
        case Grant(_, _, oi: OwnerIgnorantPermission) => 
          oi.derive(path = "/customer/cust-id")
        case Grant(_, _, oa: OwnerAwarePermission) => 
          oa.derive(path = "/customer/cust-id", owner = t.tid)
      }).map { _.gid }
      case _ => failure("Grant not found")
    }})}, timeout)
  }

  val customersCustomerAddonsGrants: Set[GrantID] = Await.result(tokens.findToken(customer.tid).flatMap { ot => Future.sequence(ot.get.grants.map { gid =>
    tokens.findGrant(gid).map { _.map {
      case Grant(_, _, oi: OwnerIgnorantPermission) => 
        (Some(gid), oi.derive(path = "/customer/cust-id"))
      case Grant(_, _, oa: OwnerAwarePermission) => 
        (Some(gid), oa.derive(path = "/customer/cust-id"))
    }}
  }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map { _.gid } } ) }}, timeout)
  
  val customersCustomerAddonAgentGrants: Set[GrantID] = Await.result(Future.sequence(addonAgentGrants.map { gid =>
    tokens.findGrant(gid).map { _.map { 
      case Grant(_, _, oi: OwnerIgnorantPermission) => 
        (Some(gid), oi.derive(path = "/customer/cust-id"))
      case Grant(_, _, oa: OwnerAwarePermission) => 
        (Some(gid), oa.derive(path = "/customer/cust-id"))
    }}
  }).flatMap { og => Future.sequence(og.collect { case Some(g) => g }.map { t => tokens.newGrant(t._1, t._2).map { _.gid } } ) }, timeout)

  tokens.addGrants(customersCustomer.tid, customersCustomerAddonsGrants ++ customersCustomerAddonAgentGrants)
}
