package com.precog.common
package security

import nsecurity._

import org.specs2.mutable._

import akka.util.Duration
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.ExecutionContext

import blueeyes.bkka.AkkaDefaults

import scala.collection

import org.joda.time.DateTime

import scalaz._

object AccessControlSpec extends Specification with TokenManagerTestValues with AccessControlHelpers with AkkaDefaults {

  implicit val accessControl = new TokenManagerAccessControl(tokens)

  "legacy access control" should {
    "control path access" in {
      "allow access" in {
        val accessRoot = mayAccessPath(rootToken.tid, "/", _: PathAccess, Set(rootToken.tid))
        val accessChild = mayAccessPath(rootToken.tid, "/child", _: PathAccess, Set(rootToken.tid))
      
        accessRoot(PathRead) must beTrue
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beTrue
        accessChild(PathWrite) must beTrue
      }
      "limit access" in {
        val accessRoot = mayAccessPath(rootLikeToken.tid, "/", _: PathAccess, Set(rootLikeToken.tid))
        val accessChild = mayAccessPath(rootLikeToken.tid, "/child", _: PathAccess, Set(rootLikeToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beTrue
      }
      "allow access via grant" in {
        val accessRoot = mayAccessPath(superToken.tid, "/", _: PathAccess, Set(rootToken.tid))
        val accessChild = mayAccessPath(superToken.tid, "/child", _: PathAccess, Set(rootToken.tid))

        accessRoot(PathRead) must beTrue
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beTrue
        accessChild(PathWrite) must beTrue
      }
      "limit access via grant" in {
        val accessRoot = mayAccessPath(childToken.tid, "/", _: PathAccess, Set(childToken.tid))
        val accessChild = mayAccessPath(childToken.tid, "/child", _: PathAccess, Set(childToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beTrue
      }
      "deny access to invalid uid" in {
        val invalidUID = "not-there"
        val accessRoot = mayAccessPath(invalidUID, "/", _: PathAccess, Set(invalidUID))
        val accessChild = mayAccessPath(invalidUID, "/child", _: PathAccess, Set(invalidUID))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access to invalid grant" in {
        val accessRoot = mayAccessPath(invalidGrantToken.tid, "/", _: PathAccess, Set(invalidGrantToken.tid))
        val accessChild = mayAccessPath(invalidGrantToken.tid, "/child", _: PathAccess, Set(invalidGrantToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer is invalid" in {
        val accessRoot = mayAccessPath(invalidGrantParentToken.tid, "/", _: PathAccess, Set(invalidGrantParentToken.tid))
        val accessChild = mayAccessPath(invalidGrantParentToken.tid, "/child", _: PathAccess, Set(invalidGrantParentToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access with no grants" in {
        val accessRoot = mayAccessPath(noPermsToken.tid, "/", _: PathAccess, Set(noPermsToken.tid))
        val accessChild = mayAccessPath(noPermsToken.tid, "/child", _: PathAccess, Set(noPermsToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant expired" in {
        val accessRoot = mayAccessPath(expiredToken.tid, "/", _: PathAccess, Set(expiredToken.tid))
        val accessChild = mayAccessPath(expiredToken.tid, "/child", _: PathAccess, Set(expiredToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant parent expired" in {
        val accessRoot = mayAccessPath(expiredParentToken.tid, "/", _: PathAccess, Set(expiredParentToken.tid))
        val accessChild = mayAccessPath(expiredParentToken.tid, "/child", _: PathAccess, Set(expiredParentToken.tid))
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
    }
    "control data access" in {
      "allow access" in {
        mayAccessData(rootToken.tid, "/", Set(rootToken.tid), DataQuery) must beTrue
        mayAccessData(rootToken.tid, "/child", Set(rootToken.tid), DataQuery) must beTrue
      }
      "limit access" in {
        mayAccessData(childToken.tid, "/", Set(childToken.tid), DataQuery) must beFalse
        mayAccessData(childToken.tid, "/child", Set(childToken.tid), DataQuery) must beTrue
      }
      "deny access to invalid uid" in {
        mayAccessData(invalidUID, "/", Set(invalidUID), DataQuery) must beFalse
        mayAccessData(invalidUID, "/child", Set(invalidUID), DataQuery) must beFalse
      }
      "deny access to invalid grant" in {
        mayAccessData(invalidGrantToken.tid, "/", Set(invalidGrantToken.tid), DataQuery) must beFalse
        mayAccessData(invalidGrantToken.tid, "/child", Set(invalidGrantToken.tid), DataQuery) must beFalse
      }
      "deny access when parent grant invalid" in {
        mayAccessData(invalidGrantParentToken.tid, "/", Set(invalidGrantParentToken.tid), DataQuery) must beFalse
        mayAccessData(invalidGrantParentToken.tid, "/child", Set(invalidGrantParentToken.tid), DataQuery) must beFalse
      }
      "deny access with no perms" in {
        mayAccessData(noPermsToken.tid, "/", Set(noPermsToken.tid), DataQuery) must beFalse
        mayAccessData(noPermsToken.tid, "/child", Set(noPermsToken.tid), DataQuery) must beFalse
      }
      "deny access when expired" in {
        mayAccessData(expiredToken.tid, "/", Set(expiredToken.tid), DataQuery) must beFalse
        mayAccessData(expiredToken.tid, "/child", Set(expiredToken.tid), DataQuery) must beFalse
      }
      "deny access when issuer expired" in {
        mayAccessData(expiredParentToken.tid, "/", Set(expiredParentToken.tid), DataQuery) must beFalse
        mayAccessData(expiredParentToken.tid, "/child", Set(expiredParentToken.tid), DataQuery) must beFalse
      }
    }
  }
}

object AccessControlUseCasesSpec extends Specification with UseCasesTokenManagerTestValues with AccessControlHelpers with AkkaDefaults {
 
  implicit val accessControl = new TokenManagerAccessControl(tokens)

  "access control" should {
    "handle proposed use cases" in {
      "addon grants sandboxed to user paths" in {
        mayAccessData(customer.tid, "/customer", Set(customer.tid), DataQuery) must beTrue
        mayAccessData(customer.tid, "/customer", Set(customer.tid), DataQuery) must beTrue
        mayAccessData(customer.tid, "/customer", Set(addon.tid), DataQuery) must beTrue
        mayAccessData(customer.tid, "/customer", Set(customer.tid, addon.tid), DataQuery) must beTrue
        
        mayAccessData(customer.tid, "/friend", Set(friend.tid), DataQuery) must beTrue
        mayAccessData(customer.tid, "/friend", Set(friend.tid, customer.tid), DataQuery) must beTrue
        
        mayAccessData(customer.tid, "/friend", Set(addon.tid), DataQuery) must beFalse
        mayAccessData(customer.tid, "/friend", Set(friend.tid, customer.tid, addon.tid), DataQuery) must beFalse
        
        mayAccessData(customer.tid, "/stranger", Set(stranger.tid), DataQuery) must beFalse
        mayAccessData(customer.tid, "/stranger", Set(addon.tid), DataQuery) must beFalse
        mayAccessData(customer.tid, "/stranger", Set(stranger.tid, addon.tid), DataQuery) must beFalse
      }
      "addon grants can be passed to our customer's customers" in {
        mayAccessData(customersCustomer.tid, "/customer/cust-id", Set(customersCustomer.tid), DataQuery) must beTrue
        mayAccessData(customersCustomer.tid, "/customer/cust-id", Set(customersCustomer.tid, addon.tid), DataQuery) must beTrue
        mayAccessData(customersCustomer.tid, "/customer", Set(customer.tid), DataQuery) must beFalse
        mayAccessData(customersCustomer.tid, "/customer", Set(customer.tid, addon.tid), DataQuery) must beFalse
      }
      "ability to access data created by an agent (child) of the granter" in {
        mayAccessData(customer.tid, "/customer", Set(customer.tid, addonAgent.tid), DataQuery) must beTrue
        mayAccessData(customersCustomer.tid, "/customer", Set(customer.tid, addonAgent.tid), DataQuery) must beFalse
        mayAccessData(customersCustomer.tid, "/customer/cust-id", Set(customersCustomer.tid, addonAgent.tid), DataQuery) must beTrue
      }
      "ability to grant revokable public access" in {
        mayAccessData(customer.tid, "/addon/public", Set(addon.tid), DataQuery) must beTrue
        mayAccessData(customer.tid, "/addon/revoked_public", Set(addon.tid), DataQuery) must beFalse
      }
    }
  }
}

trait AccessControlHelpers {

  val testTimeout = Duration(30, "seconds")

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess, owners: Set[GrantID] = Set.empty)(implicit ac: AccessControl): Boolean = {
    pathAccess match {
      case PathWrite => 
        Await.result(ac.mayAccessPath(uid, path, pathAccess), testTimeout)
      case PathRead =>
        Await.result(ac.mayAccessData(uid, path, owners, DataQuery), testTimeout)
    }
  }
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess)(implicit ac: AccessControl): Boolean = {
    Await.result(ac.mayAccessData(uid, path, owners, dataAccess), testTimeout)
  }
}

trait TokenManagerTestValues {

  val invalidUID = "invalid"

  implicit def stringToPath(path: String): Path = Path(path)
  
  val farFuture = new DateTime(Long.MaxValue)
  val farPast = new DateTime(Long.MinValue)

  val tokens = new TransientTokenManager

  def newToken(name: String)(f: NToken => Set[GrantID]): (NToken, Set[GrantID]) = {
    val token = tokens.newToken(name, Set.empty)
    val grants = f(token)
    (tokens.addGrants(token, grants), grants)
  }

  val (rootToken, rootGrants) = newToken("root") { t =>
    Grant.grantSet(None, "/", t.tid, None, Grant.ALL).map{ tokens.newGrant }.map{ _.gid }
  }
  
  val (rootLikeToken, rootLikeGrants) = newToken("rootLike") { t =>
    Grant.grantSet(None, "/child", t.tid, None, Grant.ALL).map{ tokens.newGrant }.map{ _.gid }
  }
  
  val (superToken, superGrants) = newToken("super") { t =>
    rootGrants.map{ g =>
      val rg = tokens.findGrant(g).get
      val derivedGrant = rg.grant match {
        case oi: OwnerIgnorantGrant => 
          oi.derive(issuer = Some(rg.gid))
        case oa: OwnerAwareGrant => 
          oa.derive(issuer = Some(rg.gid))
      }
      tokens.newGrant(derivedGrant).gid
    }
  }

  val (childToken, childGrants) = newToken("child") { t =>
    rootGrants.map{ g =>
      val rg = tokens.findGrant(g).get
      val derivedGrant = rg.grant match {
        case oi: OwnerIgnorantGrant => 
          oi.derive(issuer = Some(rg.gid), path = "/child")
        case oa: OwnerAwareGrant => 
          oa.derive(issuer = Some(rg.gid), path = "/child", owner = t.tid)
      }
      tokens.newGrant(derivedGrant).gid
    }
  }

  val invalidGrantID = "not going to find it"

  val (invalidGrantToken, invalidGrantGrants) = newToken("invalidGrant") { t =>
    0.until(6).map { invalidGrantID + _ }(collection.breakOut)
  }

  val (invalidGrantParentToken, invalidGrantParentGrants) = newToken("invalidGrantParent") { t =>
    Grant.grantSet(Some(invalidGrantID), "/", t.tid, None, Grant.ALL).map{ tokens.newGrant }.map{ _.gid }
  }

  val noPermsToken = tokens.newToken("noPerms", Set.empty)

  val (expiredToken, expiredGrants) = newToken("expiredGrants") { t =>
    Grant.grantSet(None, "/", t.tid, Some(farPast), Grant.ALL).map{ tokens.newGrant }.map{ _.gid }
  }

  val (expiredParentToken, expiredParentTokens) = newToken("expiredParentGrants") { t =>
    expiredGrants.map { g =>
      val rg = tokens.findGrant(g).get
      val derivedGrant = rg.grant match {
        case oi: OwnerIgnorantGrant => 
          oi.derive(issuer = Some(rg.gid), path = "/child")
        case oa: OwnerAwareGrant => 
          oa.derive(issuer = Some(rg.gid), path = "/child", owner = t.tid)
      }
      tokens.newGrant(derivedGrant).gid
    }
  }
}

trait UseCasesTokenManagerTestValues {
  implicit def stringToPath(path: String): Path = Path(path)
 

  val farFuture = new DateTime(Long.MaxValue)
  val farPast = new DateTime(Long.MinValue)

  val tokens = new TransientTokenManager

  def newToken(name: String)(f: NToken => Set[GrantID]): (NToken, Set[GrantID]) = {
    val token = tokens.newToken(name, Set.empty)
    val grants = f(token)
    (tokens.addGrants(token, grants), grants)
  }
  
  def newCustomer(name: String, parentGrants: Set[GrantID]): (NToken, Set[GrantID]) = {
    newToken(name) { t =>
      parentGrants.map{ g =>
        val rg = tokens.findGrant(g).get
        val derivedGrant = rg.grant match {
          case oi: OwnerIgnorantGrant => 
            oi.derive(issuer = Some(rg.gid), path = "/" + name)
          case oa: OwnerAwareGrant => 
            oa.derive(issuer = Some(rg.gid), path = "/", owner = t.tid)
        }
        tokens.newGrant(derivedGrant).gid
      }
    }
  }

  def addGrants(token: NToken, grants: Set[GrantID]): Option[NToken] = {
    tokens.findToken(token.tid).map { t => tokens.addGrants(t, grants); t }
  }

  val (root, rootGrants) = newToken("root") { t =>
    Grant.grantSet(None, "/", t.tid, None, Grant.ALL).map{ tokens.newGrant }.map{ _.gid }
  }

  val (customer, customerGrants) = newCustomer("customer", rootGrants)
  val (friend, friendGrants) = newCustomer("friend", rootGrants)
  val (stranger, strangerGrants) = newCustomer("stranger", rootGrants)
  val (addon, addonGrants) = newCustomer("addon", rootGrants)

  val (addonAgent, addonAgentGrants) = newToken("addon_agent") { t =>
    addonGrants.map { g =>
      val rg = tokens.findGrant(g).get
      val derivedGrant = rg.grant match {
        case oi: OwnerIgnorantGrant => 
          oi.derive(issuer = Some(rg.gid))
        case oa: OwnerAwareGrant => 
          oa.derive(issuer = Some(rg.gid), owner = t.tid)
      }
      tokens.newGrant(derivedGrant).gid
    }
  }

  val customerFriendGrants = friendGrants.map { gid =>
    tokens.findGrant(gid).flatMap { 
      case ResolvedGrant(_, rg @ ReadGrant(_, _, _, _)) =>
        Some(rg.derive(issuer = Some(gid)))
      case _ => None
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }

  val customerAddonGrants = addonGrants.map { gid =>
    tokens.findGrant(gid).flatMap { 
      case ResolvedGrant(_, rg @ ReadGrant(_, _, _, _)) =>
        Some(rg.derive(issuer = Some(gid), path = "/customer"))
      case _ => None
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }
  
  val customerAddonAgentGrants = addonAgentGrants.map { gid =>
    tokens.findGrant(gid).map { 
      case ResolvedGrant(_, oi: OwnerIgnorantGrant) => 
        oi.derive(issuer = Some(gid), path = "/customer")
      case ResolvedGrant(_, oa: OwnerAwareGrant) => 
        oa.derive(issuer = Some(gid), path = "/customer")
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }

  val customerAddonPublicGrants = addonGrants.map { gid =>
    tokens.findGrant(gid).flatMap { 
      case ResolvedGrant(_, rg @ ReadGrant(_, _, _, _)) => 
        Some(rg.derive(issuer = Some(gid), path = "/addon/public"))
      case _ => None
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }

  val customerAddonPublicRevokedGrants = addonGrants.map { gid =>
    tokens.findGrant(gid).flatMap { 
      case ResolvedGrant(_, rg @ ReadGrant(_, _, _, _)) => 
        Some(rg.derive(issuer = Some(gid), path = "/addon/public_revoked", expiration = Some(farPast)))
      case _ => None
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }

  tokens.addGrants(customer, 
    customerFriendGrants ++ 
    customerAddonGrants ++ 
    customerAddonAgentGrants ++
    customerAddonPublicGrants ++
    customerAddonPublicRevokedGrants)

  val (customersCustomer, customersCustomerGrants) = newToken("customers_customer") { t =>
    tokens.findToken(customer.tid).get.grants.map { g =>
      val rg = tokens.findGrant(g).get
      val derivedGrant = rg.grant match {
        case oi: OwnerIgnorantGrant => 
          oi.derive(issuer = Some(rg.gid), path = "/customer/cust-id")
        case oa: OwnerAwareGrant => 
          oa.derive(issuer = Some(rg.gid), path = "/customer/cust-id", owner = t.tid)
      }
      tokens.newGrant(derivedGrant).gid
    }
  }

  val customersCustomerAddonsGrants = tokens.findToken(customer.tid).get.grants.map { gid =>
    tokens.findGrant(gid).map { 
      case ResolvedGrant(_, oi: OwnerIgnorantGrant) => 
        oi.derive(issuer = Some(gid), path = "/customer/cust-id")
      case ResolvedGrant(_, oa: OwnerAwareGrant) => 
        oa.derive(issuer = Some(gid), path = "/customer/cust-id")
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }
  
  val customersCustomerAddonAgentGrants = addonAgentGrants.map { gid =>
    tokens.findGrant(gid).map { 
      case ResolvedGrant(_, oi: OwnerIgnorantGrant) => 
        oi.derive(issuer = Some(gid), path = "/customer/cust-id")
      case ResolvedGrant(_, oa: OwnerAwareGrant) => 
        oa.derive(issuer = Some(gid), path = "/customer/cust-id")
    }
  }.collect { case Some(g) => g }.map { tokens.newGrant(_).gid }

  tokens.addGrants(customersCustomer, customersCustomerAddonsGrants ++ customersCustomerAddonAgentGrants)

}
