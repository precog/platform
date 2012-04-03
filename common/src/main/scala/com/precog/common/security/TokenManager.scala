package com.precog.common
package security

import blueeyes._
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor.ActorSystem
import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import java.util.concurrent.TimeUnit._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.Validation._
import Scalaz._

trait TokenManagerComponent {
  def tokenManager: TokenManager
}

trait TokenManager {

  implicit def execContext: ExecutionContext

  def lookup(uid: UID): Future[Option[Token]]
  def lookupDeleted(uid: UID): Future[Option[Token]]
  def listChildren(parent: Token): Future[List[Token]]
  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]]
  def deleteToken(token: Token): Future[Token]
  def updateToken(token: Token): Future[Validation[String, Token]]

  def sharablePermissions(token: Token): Future[Permissions] = {
    val perms = token.permissions.sharable

    val grants = token.grants.map{ lookup }

    Future.fold(grants)(perms) {
      case (acc, grantToken) => grantToken.map { acc ++ _.permissions.sharable } getOrElse { acc }
    }
  }
  
  def permissions(token: Token): Future[Permissions] = {
    val perms = token.permissions

    val grants = token.grants.map{ lookup }

    Future.fold(grants)(perms) {
      case (acc, grantToken) => grantToken.map { acc ++ _.permissions } getOrElse { acc }
    }
  }

  private def newUUID() = java.util.UUID.randomUUID().toString.toUpperCase

  def issueNew(issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = {
    issueNew(newUUID(), issuer, permissions, grants, expired)
  }

  def listDescendants(parent: Token): Future[List[Token]] = {
    listChildren(parent) flatMap { c =>
      Future.sequence(c.map(child => listDescendants(child) map { child :: _ })).map(_.flatten)
    }
  }

  def getDescendant(parent: Token, descendantTokenId: UID): Future[Option[Token]] = {
    listDescendants(parent).map(_.find(_.uid == descendantTokenId))
  }

  def deleteDescendant(parent: Token, descendantTokenId: UID): Future[List[Token]] = {
    getDescendant(parent, descendantTokenId).flatMap { parent =>
      val removals = parent.toList.map { tok =>
        listDescendants(tok) flatMap { descendants =>
          Future.sequence((tok :: descendants) map (deleteToken))
        }
      }

      Future.sequence(removals).map(_.flatten)
    }
  }

  def updateToken(authority: Token, target: Token): Future[Validation[String, Token]] = {
    if(authority.uid == target.uid) {
      // self may not alter expiry
      if(authority.expired != target.expired) {
        updateToken(target.copy(expired = authority.expired))
      } else {
        updateToken(target)
      }
    } else {
      getDescendant(authority, target.uid).flatMap { _.map { t =>
        updateToken(target) 
      } getOrElse {
        Future(Failure("Unable to update the specified token."))
      }}
    }
  }
}

class MongoTokenManager(private[security] val database: Database, collection: String, deleted: String, timeout: Timeout)(implicit val execContext: ExecutionContext) extends TokenManager {

  private implicit val impTimeout = timeout

  def lookup(uid: UID): Future[Option[Token]] = find(uid, collection) 
  def lookupDeleted(uid: UID): Future[Option[Token]] = find(uid, deleted) 
  def updateToken(t: Token): Future[Validation[String, Token]] = sys.error("feature not available")

  private def find(uid: UID, col: String) = database {
    selectOne().from(col).where("uid" === uid)
  }.map{
    _.map(_.deserialize[Token])
  }

  def listChildren(parent: Token): Future[List[Token]] =  {
    database {
      selectAll.from(collection).where{
        "issuer" === parent.uid
      }
    }.map{ result =>
      result.toList.map(_.deserialize[Token])
    }
  }

  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = {
    lookup(uid) flatMap { 
      case Some(_) => Future("A token with this id already exists.".fail)
      case None    =>
        val newToken = Token(uid, issuer, permissions, grants, expired)
        val newTokenSerialized = newToken.serialize.asInstanceOf[JObject]
        database(insert(newTokenSerialized).into(collection)) map (_ => newToken.success)
    }
  }

  def deleteToken(token: Token): Future[Token] = for {
    _ <- database(insert(token.serialize.asInstanceOf[JObject]).into(deleted))
    _ <- database(remove.from(collection).where("uid" === token.uid))
  } yield {
    token
  }
}

trait MongoTokenManagerComponent extends Logging {
  def defaultActorSystem: ActorSystem
  implicit def execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)

  def tokenManagerFactory(config: Configuration): TokenManager = {
    val test = config[Boolean]("test", false)   
    if(test) {
      new TestTokenManager(TestTokenManager.tokenMap, execContext)
    } else {
      val mock = config[Boolean]("mongo.mock", false)
      
      val mongo = if(mock) new MockMongo else RealMongo(config.detach("mongo"))
      
      val database = config[String]("mongo.database", "auth_v1")
      val collection = config[String]("mongo.collection", "tokens")
      val deletedCollection = config[String]("mongo.deleted", collection + "_deleted")
      val timeoutMillis = config[Int]("mongo.query.timeout", 10000)

      val mongoTokenManager = 
        new MongoTokenManager(mongo.database(database), collection, deletedCollection, new Timeout(timeoutMillis))

      val cached = config[Boolean]("cached", false)

      if(cached) {
        new CachingTokenManager(mongoTokenManager)
      } else {
        mongoTokenManager
      }
    }
  }
}

class CachingTokenManager(delegate: TokenManager, settings: CacheSettings[String, Token] = CachingTokenManager.defaultSettings)(implicit val execContext: ExecutionContext) extends TokenManager {

  private val tokenCache = Cache.concurrent[String, Token](settings)

  def lookup(uid: UID): Future[Option[Token]] =
    tokenCache.get(uid) match {
      case sm @Some(t) => Future(sm: Option[Token])
      case None        => delegate.lookup(uid).map{ ot => ot ->- { _.foreach{ t => tokenCache.put(uid, t) }}}
    }
  
  def lookupDeleted(uid: UID): Future[Option[Token]] = delegate.lookupDeleted(uid)
  
  def listChildren(parent: Token): Future[List[Token]] = 
    delegate.listChildren(parent) 

  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = 
    delegate.issueNew(uid, issuer, permissions, grants, expired) 

  def updateToken(t: Token) = delegate.updateToken(t) map { _.map { t => t ->- remove } }

  def deleteToken(token: Token): Future[Token] = 
    delegate.deleteToken(token) map { t => t ->- remove }

  private def remove(t: Token) = tokenCache.remove(t.uid) 
}

object CachingTokenManager {
  val defaultSettings = CacheSettings[String, Token](ExpirationPolicy(Some(5), Some(5), MINUTES))
}


class TestTokenManager(initialTokens: Map[UID, Token], implicit val execContext: ExecutionContext) extends TokenManager {  
  var tokens = initialTokens
  var deleted = Map[UID, Token]()

  def lookup(uid: UID): Future[Option[Token]] = Future(tokens.get(uid))

  def lookupDeleted(uid: UID): Future[Option[Token]] = Future(deleted.get(uid))

  def listChildren(parent: Token): Future[List[Token]] = Future(
    tokens.values.filter {
      case Token(_, Some(parent.uid), _, _, _) => true
      case _                                   => false
    }.toList
  )
  
  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = Future {    val t = Token(uid, issuer, permissions, grants, expired)
    tokens += (t.uid -> t) 
    success(t) 
  }

  def updateToken(t: Token) = Future {
    tokens += (t.uid -> t)
    success(t) 
  }
  
  def deleteToken(token: Token): Future[Token] = Future {
    tokens.get(token.uid).foreach { t =>
      tokens -= t.uid
      deleted += (t.uid -> t)
    }     
    token
  }     

}

object TestTokenManager {
  
  val rootUID = "C18ED787-BF07-4097-B819-0415C759C8D5"
  
  val testUID = "03C4F5FE-69E2-4151-9D93-7C986936CB86"
  val usageUID = "6EF2E81E-D9E8-4DC6-AD66-DEB30A164F73"
  
  val publicUID = "B88E82F0-B78B-49D9-A36B-78E0E61C4EDC"
  val otherPublicUID = "AECEF195-81FE-4079-AF26-5B20ED32F7E0"
 
  val cust1UID = "C5EF0038-A2A2-47EB-88A4-AAFCE59EC22B"
  val cust2UID = "1B10E413-FB5B-4769-A887-8AFB587CF00A"
  
  val expiredUID = "expired"
  
  def standardAccountPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare), 
      MayAccessPath(Subtree(Path(path)), PathWrite, mayShare)
    )(
      MayAccessData(Subtree(Path("/")), HolderAndDescendants, DataQuery, mayShare)
    )

  def publishPathPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare)
    )(
      MayAccessData(Subtree(Path(path)), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  val config = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
    (rootUID, None, standardAccountPerms("/", rootUID, true), Set(), false),
    (publicUID, Some(rootUID), publishPathPerms("/public", rootUID, true), Set(), false),
    (otherPublicUID, Some(rootUID), publishPathPerms("/opublic", rootUID, true), Set(), false),
    (testUID, Some(rootUID), standardAccountPerms("/unittest", testUID, true), Set(), false),
    (usageUID, Some(rootUID), standardAccountPerms("/__usage_tracking__", usageUID, true), Set(), false),
    (cust1UID, Some(rootUID), standardAccountPerms("/user1", cust1UID, true), Set(publicUID), false),
    (cust2UID, Some(rootUID), standardAccountPerms("/user2", cust2UID, true), Set(publicUID), false),
    (expiredUID, Some(rootUID), standardAccountPerms("/expired", expiredUID, true), Set(publicUID), true)
  )

  val tokenMap = Map(config map Token.tupled map { t => (t.uid, t) }: _*)

}
