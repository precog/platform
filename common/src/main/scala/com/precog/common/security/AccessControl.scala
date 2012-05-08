package com.precog.common
package security

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Timeout

import blueeyes._
import blueeyes.bkka.AkkaDefaults
import blueeyes.json.JPath
import org.joda.time.DateTime

import scala.collection.mutable

import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._

import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import java.util.concurrent.TimeUnit._

trait AccessControl {
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean]
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean]
}

trait TokenManager {

  private[security] def newUUID() = java.util.UUID.randomUUID.toString

  private[security] def newTokenID(): String = (newUUID() + "-" + newUUID()).toUpperCase
  private[security] def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
 
  def newToken(name: String, grants: Set[GrantID]): Future[Token]
  def newGrant(grant: Grant): Future[ResolvedGrant]

  def listTokens(): Future[Seq[Token]]
  def listGrants(): Future[Seq[ResolvedGrant]]
  
  def findToken(tid: TokenID): Future[Option[Token]]
  def findGrant(gid: GrantID): Future[Option[ResolvedGrant]]
  def findGrantChildren(gid: GrantID): Future[Set[ResolvedGrant]]
  
  def listDeletedTokens(): Future[Seq[Token]]
  def listDeletedGrants(): Future[Seq[ResolvedGrant]]

  def findDeletedToken(tid: TokenID): Future[Option[Token]]
  def findDeletedGrant(gid: GrantID): Future[Option[ResolvedGrant]]
  def findDeletedGrantChildren(gid: GrantID): Future[Set[ResolvedGrant]]

  def addGrants(tid: TokenID, grants: Set[GrantID]): Future[Option[Token]]
  def removeGrants(tid: TokenID, grants: Set[GrantID]): Future[Option[Token]]

  def deleteToken(tid: TokenID): Future[Option[Token]]
  def deleteGrant(gid: GrantID): Future[Set[ResolvedGrant]]

  def close(): Future[Unit]
} 

class InMemoryTokenManager(tokens: mutable.Map[TokenID, Token] = mutable.Map.empty, 
                           grants: mutable.Map[GrantID, Grant] = mutable.Map.empty)(implicit val execContext: ExecutionContext) extends TokenManager {

  private val deletedTokens = mutable.Map.empty[TokenID, Token]
  private val deletedGrants = mutable.Map.empty[GrantID, Grant]

  def newToken(name: String, grants: Set[GrantID]) = Future {
    val newToken = Token(newTokenID, name, grants)
    tokens.put(newToken.tid, newToken)
    newToken
  }

  def newGrant(grant: Grant) = Future {
    val newGrant = ResolvedGrant(newGrantID, grant)
    grants.put(newGrant.gid, grant)
    newGrant
  }

  def listTokens() = Future[Seq[Token]] { tokens.values.toList }
  def listGrants() = Future[Seq[ResolvedGrant]] { grants.map {
    case (gid, grant) => ResolvedGrant(gid, grant)
  }.toSeq }

  def findToken(tid: TokenID) = Future { tokens.get(tid) }

  def findGrant(gid: GrantID) = Future { grants.get(gid).map { ResolvedGrant(gid, _) } }
  def findGrantChildren(gid: GrantID) = Future {
    grants.filter{ 
      case (_, grant) => grant.issuer.map { _ == gid }.getOrElse(false) 
    }.map{ ResolvedGrant.tupled.apply }(collection.breakOut)
  }

  def addGrants(tid: TokenID, add: Set[GrantID]) = Future {
    tokens.get(tid).map { t =>
      val updated = t.addGrants(add)
      tokens.put(tid, updated)
      updated
    }
  }
  
  def listDeletedTokens() = Future[Seq[Token]] { deletedTokens.values.toList }
  def listDeletedGrants() = Future[Seq[ResolvedGrant]] { deletedGrants.map {
    case (gid, grant) => ResolvedGrant(gid, grant)
  }.toSeq }
  
  def findDeletedToken(tid: TokenID) = Future { deletedTokens.get(tid) }
  
  def findDeletedGrant(gid: GrantID) = Future { deletedGrants.get(gid).map { ResolvedGrant(gid, _) } }
  def findDeletedGrantChildren(gid: GrantID) = Future {
    deletedGrants.filter{ 
      case (_, grant) => grant.issuer.map { _ == gid }.getOrElse(false) 
    }.map{ ResolvedGrant.tupled.apply }(collection.breakOut)
  }

 def removeGrants(tid: TokenID, remove: Set[GrantID]) = Future {
    tokens.get(tid).map { t =>
      val updated = t.removeGrants(remove)
      tokens.put(tid, updated)
      updated
    }
  }

  def deleteToken(tid: TokenID) = Future { 
    tokens.get(tid).flatMap { t =>
      deletedTokens.put(tid, t)
      tokens.remove(tid)
    }
  }
  def deleteGrant(gid: GrantID) = Future { grants.remove(gid) match {
    case Some(x) => 
      deletedGrants.put(gid, x)
      Set(ResolvedGrant(gid, x))
    case _       => 
      Set.empty
  }}

  def close() = Future(())
}

object TestTokenManager extends AkkaDefaults {
  
  val rootUID = "root"
  
  val testUID = "test"
  val usageUID = "usage"
  
//  val publicUID = "B88E82F0-B78B-49D9-A36B-78E0E61C4EDC"
//  val otherPublicUID = "AECEF195-81FE-4079-AF26-5B20ED32F7E0"
 
  val cust1UID = "user1"
  val cust2UID = "user2"
  
  val expiredUID = "expired"
  
//  def standardAccountPerms(path: String, mayShare: Boolean = true) =
//    Permissions(
//      MayAccessPath(Subtree(Path(path)), PathRead, mayShare), 
//      MayAccessPath(Subtree(Path(path)), PathWrite, mayShare)
//    )(
//      MayAccessData(Subtree(Path("/")), HolderAndDescendants, DataQuery, mayShare)
//    )
//
//  def publishPathPerms(path: String, owner: UID, mayShare: Boolean = true) =
//    Permissions(
//      MayAccessPath(Subtree(Path(path)), PathRead, mayShare)
//    )(
//      MayAccessData(Subtree(Path(path)), OwnerAndDescendants(owner), DataQuery, mayShare)
//    )
//
//  val config = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
//    (rootUID, None, standardAccountPerms("/", true), Set(), false),
//    (publicUID, Some(rootUID), publishPathPerms("/public", rootUID, true), Set(), false),
//    (otherPublicUID, Some(rootUID), publishPathPerms("/opublic", rootUID, true), Set(), false),
//    (testUID, Some(rootUID), standardAccountPerms("/unittest", true), Set(), false),
//    (usageUID, Some(rootUID), standardAccountPerms("/__usage_tracking__", true), Set(), false),
//    (cust1UID, Some(rootUID), standardAccountPerms("/user1", true), Set(publicUID), false),
//    (cust2UID, Some(rootUID), standardAccountPerms("/user2", true), Set(publicUID), false),
//    (expiredUID, Some(rootUID), standardAccountPerms("/expired", true), Set(publicUID), true)
//  )
//
//  val tokenMap = Map(config map Token.tupled map { t => (t.uid, t) }: _*)

  def standardAccountPerms(prefix: String, issuerPrefix: Option[String], p: String, owner: TokenID, expiration: Option[DateTime]) = {
    val path = Path(p)
    List(
      ResolvedGrant(prefix + "_write", WriteGrant(issuerPrefix.map{ _ + "_write" }, path, expiration)), 
      ResolvedGrant(prefix + "_owner", OwnerGrant(issuerPrefix.map{ _ + "_owner" }, path, expiration)), 
      ResolvedGrant(prefix + "_read", ReadGrant(issuerPrefix.map{ _ + "_read" }, path, owner, expiration)), 
      ResolvedGrant(prefix + "_reduce", ReduceGrant(issuerPrefix.map{ _ + "_reduce" }, path, owner, expiration)), 
      ResolvedGrant(prefix + "_modify", ModifyGrant(issuerPrefix.map{ _ + "_modify" }, path, owner, expiration)), 
      ResolvedGrant(prefix + "_transform", TransformGrant(issuerPrefix.map{ _ + "_transform" }, path, owner, expiration)) 
    )
  }
  
  def publishPathPerms(prefix: String, issuerPrefix: Option[String], p: String, owner: TokenID, expiration: Option[DateTime]) = {
    val path = Path(p)
    List(
      ResolvedGrant(prefix + "_read", ReadGrant(issuerPrefix.map{ _ + "_read" }, path, owner, expiration)), 
      ResolvedGrant(prefix + "_reduce", ReduceGrant(issuerPrefix.map{ _ + "_reduce" }, path, owner, expiration)) 
    )
  }

  val grantList = List(
    standardAccountPerms("root", None, "/", "root", None), 
    standardAccountPerms("unittest", Some("root"), "/unittest", "unittest", None),
    standardAccountPerms("__usage_tracking__", Some("root"), "/__usage_tracking__", "usage_tracking", None),
    standardAccountPerms("user1", Some("root"), "/user1", "user1", None),
    standardAccountPerms("user2", Some("root"), "/user2", "user2", None),
    standardAccountPerms("expired", Some("root"), "/expired", "expired", Some(new DateTime().minusYears(1000))),
    publishPathPerms("public", Some("root"), "/public", "public", None),
    publishPathPerms("opublic", Some("root"), "/opublic", "opublic", None)
  )

  val grants: mutable.Map[GrantID, Grant] = grantList.flatten.map { g => (g.gid -> g.grant) }(collection.breakOut)
  
  val tokens: mutable.Map[TokenID, Token] = List[Token](
    Token("root", "root", grantList(0).map { _.gid }(collection.breakOut)),
    Token("test", "test", grantList(1).map { _.gid }(collection.breakOut)),
    Token("usage", "usage", grantList(2).map { _.gid }(collection.breakOut)),
    Token("user1", "user1", (grantList(3) ++ grantList(6)).map{ _.gid}(collection.breakOut)),
    Token("user2", "user2", (grantList(4) ++ grantList(6)).map{ _.gid}(collection.breakOut)),
    Token("expired", "expired", (grantList(5) ++ grantList(6)).map{ _.gid}(collection.breakOut))
  ).map { t => (t.tid -> t) }(collection.breakOut)


  def testTokenManager(): TokenManager = new InMemoryTokenManager(tokens, grants)

}

case class MongoTokenManagerSettings(
  tokens: String = "tokens",
  grants: String = "grants",
  deletedTokens: String = "tokens_deleted",
  deletedGrants: String = "grants_deleted",
  timeout: Timeout = new Timeout(30000))

object MongoTokenManagerSettings {
  val defaults = MongoTokenManagerSettings()
}

class MongoTokenManager(
    private[security] val mongo: Mongo, 
    private[security] val database: Database, 
    settings: MongoTokenManagerSettings = MongoTokenManagerSettings.defaults)(implicit val execContext: ExecutionContext) extends TokenManager {

  private implicit val impTimeout = settings.timeout
  
  def newToken(name: String, grants: Set[GrantID]) = {
    val newToken = Token(newTokenID, name, grants)
    database(insert(newToken.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]).into(settings.tokens)) map {
      _ => newToken 
    }
  }

  def newGrant(grant: Grant) = {
    val ng = ResolvedGrant(newGrantID, grant)
    database(insert(ng.serialize(ResolvedGrant.UnsafeResolvedGrantDecomposer).asInstanceOf[JObject]).into(settings.grants)) map { _ => ng }
  }

  private def findOneMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Option[A]] = {
    database {
      selectOne().from(collection).where(keyName === keyValue)
    }.map {
      _.map(_.deserialize(extractor))
    }
  }

  private def findAllMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database {
      selectAll.from(collection).where(keyName === keyValue)
    }.map {
      _.map(_.deserialize(extractor)).toSet
    }
  }

  private def findAll[A](collection: String)(implicit extract: Extractor[A]): Future[Seq[A]] =
    database { selectAll.from(collection) }.map { _.map(_.deserialize(extract)).toSeq }

  def listTokens() = findAll[Token](settings.tokens)
  def listGrants() = findAll[ResolvedGrant](settings.grants)

  def findToken(tid: TokenID) = findOneMatching[Token]("tid", tid, settings.tokens)
  def findGrant(gid: GrantID) = findOneMatching[ResolvedGrant]("gid", gid, settings.grants)
  
  def findGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.grants)
  
  def listDeletedTokens() = findAll[Token](settings.tokens)
  def listDeletedGrants() = findAll[ResolvedGrant](settings.grants)
  
  def findDeletedToken(tid: TokenID) = findOneMatching[Token]("tid", tid, settings.deletedTokens)
  def findDeletedGrant(gid: GrantID) = findOneMatching[ResolvedGrant]("gid", gid, settings.deletedGrants)
  
  def findDeletedGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.deletedGrants)

  def addGrants(tid: TokenID, add: Set[GrantID]) = updateToken(tid) { t =>
    Some(t.addGrants(add))
  }
  
  def removeGrants(tid: TokenID, remove: Set[GrantID]) = updateToken(tid) { t =>
    Some(t.removeGrants(remove))
  }

  private def updateToken(tid: TokenID)(f: Token => Option[Token]): Future[Option[Token]] = {
    findToken(tid).flatMap {
      case Some(t) =>
        f(t) match {
          case Some(nt) if nt != t => 
            database {
              val updateObj = nt.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]
              update(settings.tokens).set(updateObj).where("tid" === tid)
            }.map{ _ => Some(nt) }
          case _ => Future(Some(t))
        }
      case None    => Future(None)
    }
  }
  

  def deleteToken(tid: TokenID): Future[Option[Token]] = 
    findToken(tid).flatMap { 
      case ot @ Some(t) => 
        for {
          _ <- database(insert(t.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]).into(settings.deletedTokens))
          _ <- database(remove.from(settings.tokens).where("tid" === tid))
        } yield { ot }
      case None    => Future(None)
    } 

  def deleteGrant(gid: GrantID): Future[Set[ResolvedGrant]] = {
    findGrantChildren(gid).flatMap { gc => 
      Future.sequence(gc.map { g => deleteGrant(g.gid)}).map { _.flatten }.flatMap { gds =>
        findGrant(gid).flatMap { 
          case og @ Some(g) => 
            for {
              _ <- database(insert(g.serialize(ResolvedGrant.UnsafeResolvedGrantDecomposer).asInstanceOf[JObject]).into(settings.deletedGrants))
              _ <- database(remove.from(settings.grants).where("gid" === gid))
            } yield { gds + g }
          case None    => Future(gds)
        }
      }
    }
  }

  def close() = database.disconnect.fallbackTo(Future(())).flatMap{_ => mongo.close}
}

case class CachingTokenManagerSettings(
  tokenCacheSettings: CacheSettings[TokenID, Token],
  grantCacheSettings: CacheSettings[GrantID, Grant])

class CachingTokenManager(manager: TokenManager, settings: CachingTokenManagerSettings = CachingTokenManager.defaultSettings)(implicit execContext: ExecutionContext) extends TokenManager {
  
  private val tokenCache = Cache.concurrent[TokenID, Token](settings.tokenCacheSettings)
  private val grantCache = Cache.concurrent[GrantID, Grant](settings.grantCacheSettings)

  def newToken(name: String, grants: Set[GrantID]) =
    manager.newToken(name, grants).map { _ ->- add }
  def newGrant(grant: Grant) =
    manager.newGrant(grant).map { _ ->- add }

  def listTokens() = manager.listTokens
  def listGrants() = manager.listGrants
  
  def listDeletedTokens() = manager.listDeletedTokens
  def listDeletedGrants() = manager.listDeletedGrants

  def findToken(tid: TokenID) = tokenCache.get(tid) match {
    case None => manager.findToken(tid).map { _.map { _ ->- add } }
    case t    => Future(t)
  }
  def findGrant(gid: GrantID) = grantCache.get(gid) match {
    case None    => manager.findGrant(gid).map { _.map { _ ->- add } }
    case Some(g) => Future(Some(ResolvedGrant(gid, g)))
  }
  def findGrantChildren(gid: GrantID) = manager.findGrantChildren(gid)
  
  def findDeletedToken(tid: TokenID) = manager.findDeletedToken(tid)
  def findDeletedGrant(gid: GrantID) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantID) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: TokenID, grants: Set[GrantID]) =
    manager.addGrants(tid, grants).map { _.map { _ ->- add } }
  def removeGrants(tid: TokenID, grants: Set[GrantID]) =
    manager.removeGrants(tid, grants).map { _.map { _ ->- add } } 

  def deleteToken(tid: TokenID) =
    manager.deleteToken(tid) map { _.map { _ ->- remove } }
  def deleteGrant(gid: GrantID) =
    manager.deleteGrant(gid) map { _.map { _ ->- remove } }

  private def add(t: Token) = tokenCache.put(t.tid, t)
  private def add(g: ResolvedGrant) = grantCache.put(g.gid, g.grant)

  private def remove(t: Token) = tokenCache.remove(t.tid)
  private def remove(g: ResolvedGrant) = grantCache.remove(g.gid)

  def close() = manager.close

}

object CachingTokenManager {
  val defaultSettings = CachingTokenManagerSettings(
    CacheSettings[TokenID, Token](ExpirationPolicy(Some(5), Some(5), MINUTES)),
    CacheSettings[GrantID, Grant](ExpirationPolicy(Some(5), Some(5), MINUTES))
  )
}

class TokenManagerAccessControl(tokens: TokenManager)(implicit execContext: ExecutionContext) extends AccessControl {

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean] = 
    pathAccess match {
      case PathRead => mayAccess(uid, path, Set(uid), ReadGrant) 
      case PathWrite => mayAccess(uid, path, Set.empty, WriteGrant)
    }

  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean] = 
    mayAccess(uid, path, owners, ReadGrant)
 
  def mayAccess(uid: TokenID, path: Path, owners: Set[UID], accessType: AccessType): Future[Boolean] = {
    tokens.findToken(uid).flatMap{ _.map { t => 
       hasValidGrants(t, path, owners, accessType)
    }.getOrElse(Future(false)) }
  }

  def hasValidGrants(t: Token, path: Path, owners: Set[UID], accessType: AccessType): Future[Boolean] = {

    def exists(fs: Iterable[Future[Boolean]]): Future[Boolean] = {
      if(fs.size == 0) Future(false)
      else Future.reduce(fs){ case (a,b) => a || b }
    }
    
    def forall(fs: Iterable[Future[Boolean]]): Future[Boolean] = {
      if(fs.size == 0) Future(false)
      else Future.reduce(fs){ case (a,b) => a && b }
    }

    accessType match {
      case WriteGrant =>
        exists(t.grants.map{ gid =>
          tokens.findGrant(gid).flatMap( _.map { 
            case ResolvedGrant(_, grant @ WriteGrant(_, _, _)) =>
              isValid(grant).map { _ && grant.path.equalOrChild(path) }
            case _ => Future(false)
          }.getOrElse(Future(false))
        )})
      case OwnerGrant =>
        exists(t.grants.map{ gid =>
          tokens.findGrant(gid).flatMap( _.map { 
            case ResolvedGrant(_, grant @ OwnerGrant(_, _, _)) =>
              isValid(grant).map { _ && grant.path.equalOrChild(path) }
            case _ => Future(false)
          }.getOrElse(Future(false))
        )})
      case ReadGrant =>
        if(owners.isEmpty) Future(false)
        else forall(owners.map { owner =>
          exists(t.grants.map{ gid =>
            tokens.findGrant(gid).flatMap( _.map {
              case ResolvedGrant(_, grant @ ReadGrant(_, _, o, _)) =>
                isValid(grant).map { _ && grant.path.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
      case ReduceGrant =>
        if(owners.isEmpty) Future(false)
        else forall( owners.map { owner =>
          exists( t.grants.map{ gid =>
            tokens.findGrant(gid).flatMap( _.map { 
              case ResolvedGrant(_, grant @ ReduceGrant(_, _, o, _)) =>
                isValid(grant).map { _ && grant.path.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
      case ModifyGrant =>
        if(owners.isEmpty) Future(false)
        else forall(owners.map { owner =>
          exists(t.grants.map { gid =>
            tokens.findGrant(gid).flatMap( _.map {
              case ResolvedGrant(_, grant @ ModifyGrant(_, _, o, _)) =>
                isValid(grant).map { _ && grant.path.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
      case TransformGrant =>
        if(owners.isEmpty) Future(false)
        else forall(owners.map { owner =>
          exists(t.grants.map { gid =>
            tokens.findGrant(gid).flatMap( _.map { 
              case ResolvedGrant(_, grant @ TransformGrant(_, _, o, _)) =>
                isValid(grant).map { _ && grant.path.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
    }
  }

  def isValid(grant: Grant): Future[Boolean] = {
    (grant.issuer.map { 
      tokens.findGrant(_).flatMap { _.map { parentGrant => 
        isValid(parentGrant.grant).map { _ && grant.accessType == parentGrant.grant.accessType }
      }.getOrElse(Future(false)) } 
    }.getOrElse(Future(true))).map { _ && !grant.isExpired(new DateTime()) }
  }
}

class UnlimitedAccessControl(implicit executionContext: ExecutionContext) extends AccessControl {
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess) = Future(true)
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess) = Future(true)
}

//trait TokenBasedAccessControl extends AccessControl with TokenManagerComponent {
//
//  implicit def executionContext: ExecutionContext
//
//  def mayGrant(uid: UID, permissions: Permissions) = {
//    tokenManager.lookup(uid) flatMap {
//      case None => Future(false)
//      case Some(t) =>
//        val pathsValid = Future.reduce(permissions.path.map { p => validPathGrant(t, p) }){ _ && _ }
//        val dataValid = Future.reduce(permissions.data.map { p => validDataGrant(t, p) }){ _ && _ }
//            
//        (pathsValid zip dataValid) map { t => t._1 && t._2 }
//    }
//  }
//
//  private def validPathGrant(token: Token, path: MayAccessPath): Future[Boolean] = Future {
//    token.permissions.sharable.path.map { ref =>
//      pathSpecSubset(ref.pathSpec, path.pathSpec) && ref.pathAccess == path.pathAccess
//    }.reduce{ _ || _ }
//  }
//
//  private def sharablePermissions(token: Token): Future[Permissions] = {
//
//    val foldPerms = Future.fold(_: Set[Future[Permissions]])(token.permissions.sharable) {
//      case (acc, el) => acc ++ el
//    }
//
//    val extractPerms = (_: Token).grants.map { tokenManager.lookup(_).map {
//      case Some(t) => t.permissions.sharable
//      case None    => Permissions.empty
//    }}
//
//    extractPerms andThen foldPerms apply token
//  }
//  
//  private def validDataGrant(token: Token, data: MayAccessData): Future[Boolean] = {
//    val tests: Set[Future[Boolean]] = token.permissions.sharable.data.map { ref =>
//      ownerSpecSubset(token.uid, ref.ownershipSpec, token.uid, data.ownershipSpec) map {
//        _ && pathSpecSubset(ref.pathSpec, data.pathSpec) && ref.dataAccess == data.dataAccess
//      }
//    }
//    Future.reduce(tests.toList){ _ || _ }
//  }
//
//  private def pathSpecSubset(ref: PathRestriction, test: PathRestriction): Boolean =
//    (ref, test) match {
//      case (Subtree(r), Subtree(t)) => r.equalOrChild(t)
//      case _                        => false
//    }
//
//  private def ownerSpecSubset(refUID: UID, ref: OwnerRestriction, testUID: UID, test: OwnerRestriction): Future[Boolean] = {
//
//    def effectiveUID(ownerSpec: OwnerRestriction, holder: UID) = ownerSpec match {
//      case OwnerAndDescendants(o) => o
//      case HolderAndDescendants   => holder 
//    }
//
//    def childOrEqual(ruid: UID, tuid: UID) = 
//      if(ruid == tuid) {
//        Future(true)
//      } else {
//        tokenManager.lookup(ruid) flatMap {
//          case None    => Future(false)
//          case Some(rt) =>
//            tokenManager.getDescendant(rt, tuid) map { _.isDefined }
//          }
//      }
//
//    childOrEqual(effectiveUID(ref, refUID), effectiveUID(test, testUID))
//  }
//
//  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean] = {
//    mayAccessPath(uid, path, pathAccess, false)
//  }
//
//  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess, sharingRequired: Boolean): Future[Boolean] = {
//    tokenManager lookup(uid) flatMap { _.map { mayAccessPath(_, path, pathAccess, sharingRequired) } getOrElse { Future(false) } }
//  }
//
//  def sharedIfRequired(sharingRequired: Boolean, mayShare: Boolean) = !(sharingRequired && !mayShare)
//
//  def mayAccessPath(token: Token, testPath: Path, testPathAccess: PathAccess, sharingRequired: Boolean): Future[Boolean] = {
//
//    type Perm = (Option[UID], MayAccessPath)
//
//    def findMatchingPermission: Future[Set[Perm]] = {
//      def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessPath]): Set[Perm] =
//        perms collect {
//          case perm @ MayAccessPath(pathSpec, pathAccess, mayShare) 
//            if pathAccess == testPathAccess && 
//             pathSpec.matches(testPath) &&
//             sharedIfRequired(sharingRequired, mayShare) => (issuer, perm)
//        }
//      
//      val localPermissions = extractMatchingPermissions(token.issuer, token.permissions.path)
//      
//      val grantedPermissions: Future[Set[Perm]] = {
//        val nested: Set[Future[Set[Perm]]] = token.grants map {
//          tokenManager lookup(_) map { 
//            _.map{ grantToken =>
//              if(grantToken.isValid) {
//                extractMatchingPermissions(grantToken.issuer, grantToken.permissions.path)     
//              } else {
//                Set.empty[Perm] 
//              }
//            }.getOrElse(Set.empty[Perm])
//          }
//        }
//        Future.sequence(nested).map{ _.flatten }
//      }
//
//      grantedPermissions map { localPermissions ++ _ }
//    } 
//
//    def hasValidatedPermission: Future[Boolean] = {
//      findMatchingPermission flatMap { perms =>
//        if(perms.size == 0) { Future(false) } else {
//          Future.reduce(perms.map {
//            case (None, perm)         => Future(true)
//            case (Some(issuer), perm) => mayAccessPath(issuer, testPath, testPathAccess, true)
//          })(_ || _)
//        }
//      }
//    }
//    
//    hasValidatedPermission map { _ && token.isValid } 
//  }
// 
//  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean] = {
//    mayAccessData(uid, path, owners, dataAccess, false)
//  }
//
//  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess, sharingRequired: Boolean): Future[Boolean] = {
//    tokenManager lookup(uid) flatMap { _.map { mayAccessData(_, path, owners, dataAccess, sharingRequired) }.getOrElse{ Future(false) } }
//  }
//
//  def mayAccessData(token: Token, testPath: Path, testOwners: Set[UID], testDataAccess: DataAccess, sharingRequired: Boolean): Future[Boolean] = {
//  
//    type Perm = (Option[UID], MayAccessData)
//
//    def findMatchingPermission(holderUID: UID, testOwner: UID): Future[Set[Perm]] = {
//     
//      // start work here!!!!
//
//      def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessData]): Future[Set[Perm]] = { 
//        val flagged = perms map {
//          case perm @ MayAccessData(pathSpec, ownerSpec, dataAccess, mayShare)
//            if dataAccess == testDataAccess && 
//             pathSpec.matches(testPath) &&
//             sharedIfRequired(sharingRequired, mayShare) =>
//             checkOwnershipRestriction(holderUID, testOwner, ownerSpec) map { b => (b, (issuer, perm)) }
//          case perm @ MayAccessData(_,_,_,_) => Future((false, (issuer, perm)))
//        }
//        Future.fold(flagged)(Set.empty[Perm]){
//          case (acc, (b, t)) => if(b) acc + t else acc
//        }
//      }
//      
//      
//      val localPermissions = extractMatchingPermissions(token.issuer, token.permissions.data)
//      
//      val grantedPermissions: Future[Set[Perm]] = {
//        val nested: Set[Future[Set[Perm]]] = token.grants map {
//          tokenManager lookup(_) flatMap { 
//            _.map{ grantToken =>
//              if(grantToken.isValid) {
//                extractMatchingPermissions(grantToken.issuer, grantToken.permissions.data)     
//              } else {
//                Future(Set.empty[Perm])
//              }
//            }.getOrElse(Future(Set.empty[Perm]))
//          }
//        }
//        Future.sequence(nested).map{ _.flatten }
//      }
//
//
//      localPermissions flatMap { lp => grantedPermissions map { lp ++ _ }}
//    } 
//
//    def hasValidatedPermission(holderUID: UID): Future[Boolean] = {
//      Future.reduce(testOwners map { testOwner =>
//        findMatchingPermission(holderUID, testOwner) flatMap { perms =>
//          if(perms.size == 0) { Future(false) } else {
//            Future.reduce(perms.map { 
//              case (None, perm)         => Future(true)
//              case (Some(issuer), perm) => mayAccessData(issuer, testPath, Set(testOwner), testDataAccess, true)
//            })(_ || _)
//          }
//        }
//      })(_ && _)
//    }
//
//    hasValidatedPermission(token.uid) map { _ && token.isValid } 
//  }
//
//  def checkOwnershipRestriction(grantHolder: UID, testOwner: UID, restriction: OwnerRestriction): Future[Boolean] = restriction match {
//    case OwnerAndDescendants(owner) => isChildOf(owner, testOwner)
//    case HolderAndDescendants       => isChildOf(grantHolder, testOwner)
//  }
//  
//  def isChildOf(parent: UID, possibleChild: UID): Future[Boolean] = 
//    if(parent == possibleChild) 
//      Future(true)
//    else
//      tokenManager lookup(possibleChild) flatMap { 
//        _.flatMap {
//          _.issuer.map { isChildOf(parent, _) }
//        } getOrElse(Future(false))
//      } 
//
//}
