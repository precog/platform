package com.precog.common
package security

import org.specs2.mutable._

import akka.util.Duration
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.ExecutionContext

import blueeyes.bkka.AkkaDefaults

import scalaz._

object AccessControlSpec extends Specification with AccessControlTestValues with AccessControlHelpers with AkkaDefaults {
  implicit val accessControl = new TokenBasedAccessControl with TestTokenManagerComponent {
    implicit val executionContext = defaultFutureDispatch
    val tokenConfig = accessControlTokenConfig
  }

  "access control" should {
    "control path access" in {
      "allow access" in {
        val accessRoot = mayAccessPath(rootUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(rootUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beTrue
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beTrue
        accessChild(PathWrite) must beTrue
      }
      "allow access via grant" in {
        val accessRoot = mayAccessPath(grantUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(grantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beTrue
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beTrue
        accessChild(PathWrite) must beTrue
      }
      "limit access to constrained perms" in {
        val accessRoot = mayAccessPath(childUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(childUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beTrue
      }
      "limit access via grant" in {
        val accessRoot = mayAccessPath(limitedGrantUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(limitedGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beTrue
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beTrue
      }
      "deny access to invalid uid" in {
        val accessRoot = mayAccessPath(invalidUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(invalidUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer uid is invalid" in {
        val accessRoot = mayAccessPath(invalidChildUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(invalidChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access to invalid grant" in {
        val accessRoot = mayAccessPath(invalidGrantUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(invalidGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer is invalid" in {
        val accessRoot = mayAccessPath(invalidGrantChildUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(invalidGrantChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access with no perms" in {
        val accessRoot = mayAccessPath(noPermsUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(noPermsUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer no longer has perms" in {
        val accessRoot = mayAccessPath(noPermsChildUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(noPermsChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer no longer has perms" in {
        val accessRoot = mayAccessPath(noPermsGrantUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(noPermsGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer may not share perms" in {
        val accessRoot = mayAccessPath(noShareChildUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(noShareChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer may not share perms" in {
        val accessRoot = mayAccessPath(noShareGrantUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(noShareGrantUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when expired" in {
        val accessRoot = mayAccessPath(expiredUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(expiredUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant expired" in {
        val accessRoot = mayAccessPath(grantExpiredUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(grantExpiredUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when issuer expired" in {
        val accessRoot = mayAccessPath(expiredChildUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(expiredChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
      "deny access when grant issuer expired" in {
        val accessRoot = mayAccessPath(grantExpiredChildUID, "/", _: PathAccess)
        val accessChild = mayAccessPath(grantExpiredChildUID, "/child", _: PathAccess)
      
        accessRoot(PathRead) must beFalse
        accessChild(PathRead) must beFalse
        accessRoot(PathWrite) must beFalse
        accessChild(PathWrite) must beFalse
      }
    }
    "control data access" in {
      "allow access" in {
        mayAccessData(rootUID, "/", Set(rootUID), DataQuery) must beTrue
        mayAccessData(rootUID, "/child", Set(rootUID), DataQuery) must beTrue
      }
      "limit access" in {
        mayAccessData(childUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(childUID, "/child", Set(rootUID), DataQuery) must beTrue
      }
      "deny access to invalid uid" in {
        mayAccessData(invalidUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(invalidUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer uid is invalid" in {
        mayAccessData(invalidChildUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(invalidChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access with no perms" in {
        mayAccessData(noDataPermsUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(noDataPermsUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer no longer has perms" in {
        mayAccessData(noDataPermsChildUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(noDataPermsChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer may not perms" in {
        mayAccessData(noDataShareChildUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(noDataShareChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when expired" in {
        mayAccessData(expiredUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(expiredUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
      "deny access when issuer expired" in {
        mayAccessData(expiredChildUID, "/", Set(rootUID), DataQuery) must beFalse
        mayAccessData(expiredChildUID, "/child", Set(rootUID), DataQuery) must beFalse
      }
    }
  }
}

object AccessControlUseCasesSpec extends Specification with AccessControlTestValues with AccessControlHelpers with AkkaDefaults {
 
  implicit val accessControl = new TokenBasedAccessControl with TestTokenManagerComponent {
    implicit val executionContext = defaultFutureDispatch
    val tokenConfig = useCaseTokenConfig
  }

  "access control" should {
    "handle proposed use cases" in {
      "addon grants sandboxed to user paths" in {
        mayAccessPath(customerUID, "/customer", PathRead) must beTrue
        mayAccessPath(customerUID, "/knownCustomer", PathRead) must beTrue
        mayAccessPath(customerUID, "/unknownCustomer", PathRead) must beFalse

        mayAccessData(customerUID, "/customer", Set(customerUID), DataQuery) must beTrue
        mayAccessData(customerUID, "/customer", Set(customerUID, addonUID), DataQuery) must beTrue
        mayAccessData(customerUID, "/knownCustomer", Set(knownCustomerUID), DataQuery) must beTrue
        mayAccessData(customerUID, "/knownCustomer", Set(knownCustomerUID, customerUID), DataQuery) must beTrue

        mayAccessData(customerUID, "/knownCustomer", Set(knownCustomerUID, customerUID, addonCustomerGrantUID), DataQuery) must beFalse
        mayAccessData(customerUID, "/unknownCustomer", Set(unknownCustomerUID), DataQuery) must beFalse
        mayAccessData(customerUID, "/unknownCustomer", Set(unknownCustomerUID, addonCustomerGrantUID), DataQuery) must beFalse
        mayAccessData(customerUID, "/unknownCustomer", Set(addonCustomerGrantUID), DataQuery) must beFalse
      }
      "addon grants can be passed to our customer's customers" in {
        mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID), DataQuery) must beTrue
        mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID, addonUID), DataQuery) must beTrue
        mayAccessData(customersCustomerUID, "/customer", Set(customerUID), DataQuery) must beFalse
        mayAccessData(customersCustomerUID, "/customer", Set(customerUID, addonUID), DataQuery) must beFalse
      }
      "ability to access data created by an agent (child) of the granter" in {
        mayAccessData(customerUID, "/customer", Set(customerUID, addonAgentUID), DataQuery) must beTrue
        mayAccessData(customersCustomerUID, "/customer", Set(customerUID, addonAgentUID), DataQuery) must beFalse
        mayAccessData(customersCustomerUID, "/customer/cust-id", Set(customersCustomerUID, addonAgentUID), DataQuery) must beTrue
      }
      "ability to grant revokable public access" in {
        mayAccessData(customerUID, "/addon/public", Set(addonUID), DataQuery) must beTrue
      }
    }
  }
}

trait AccessControlHelpers {

  val testTimeout = Duration(30, "seconds")

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess)(implicit ac: AccessControl): Boolean = {
    Await.result(ac.mayAccessPath(uid, path, pathAccess), testTimeout)
  }
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess)(implicit ac: AccessControl): Boolean = {
    Await.result(ac.mayAccessData(uid, path, owners, dataAccess), testTimeout)
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

trait TestTokenManagerComponent extends TokenManagerComponent with AkkaDefaults {

  def tokenConfig: List[(UID, Option[UID], Permissions, Set[UID], Boolean)]

  lazy val map = Map( tokenConfig map {
    case (uid, issuer, perms, grants, canShare) => (uid -> Token(uid, issuer, perms, grants, canShare))
  }: _*)

  lazy val tokenManager = new TokenManager {
    implicit val execContext = defaultFutureDispatch
    def list() = sys.error("not available")
    def lookup(uid: UID) = Future(map.get(uid))(execContext)
    def lookupDeleted(uid: UID) = sys.error("not available")
    def listChildren(parent: Token): Future[List[Token]] = sys.error("not available")
    def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = sys.error("not available")
    def deleteToken(token: Token): Future[Token] = sys.error("not available")
    def updateToken(token: Token): Future[Validation[String, Token]] = sys.error("not available")
  }
}
