package com.reportgrid.analytics

import blueeyes._
import blueeyes.BlueEyesServiceBuilder
import blueeyes.concurrent._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import net.lag.configgy.ConfigMap

import org.joda.time.DateTime

import java.util.concurrent.TimeUnit._

import scala.util.matching.Regex
import scala.math._
import scalaz.Scalaz._
import scalaz.Validation

trait TokenStorage {
  def lookup(tokenId: String): Future[Option[Token]]
  def listChildren(parent: Token): Future[List[Token]]
  def issueNew(parent: Token, path: Path, permissions: Permissions, expires: DateTime, limits: Limits): Future[Validation[String, Token]]
  /** List all descendants of the specified token.  */
  def listDescendants(parent: Token): Future[List[Token]] = {
    listChildren(parent) flatMap { 
      _.map(child => listDescendants(child) map { child :: _ }).sequence.map(_.flatten)
    }
  }

  /** Get details about a specified child token.
   */
  def getDescendant(parent: Token, descendantTokenId: String): Future[Option[Token]] = {
    listDescendants(parent).map(_.find(_.tokenId == descendantTokenId))
  }

  /** Delete a specified child token and all of its descendants.
   */
  def deleteDescendant(auth: Token, descendantTokenId: String): Future[List[Token]] = {
    getDescendant(auth, descendantTokenId).flatMap { parent =>
      val removals = parent.toList.map { tok => 
        listDescendants(tok) flatMap { descendants =>
          (tok :: descendants) map (deleteToken) sequence
        }
      }
    
      removals.sequence.map(_.flatten)
    }
  }

  protected def deleteToken(token: Token): Future[Token]
}

class TokenManager (database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection) extends TokenStorage {
  //TODO: Add expiry settings.
  val tokenCache = Cache.concurrent[String, Token](CacheSettings(ExpirationPolicy(None, None, MILLISECONDS)))
  tokenCache.put(Token.Root.tokenId, Token.Root)
  tokenCache.put(Token.Test.tokenId, Token.Test)

  private def find(tokenId: String) = database {
    selectOne().from(tokensCollection).where("tokenId" === tokenId)
  }

  /** Look up the specified token.
   */
  def lookup(tokenId: String): Future[Option[Token]] = {
    tokenCache.get(tokenId).map[Future[Option[Token]]](v => Future.sync(Some(v))) getOrElse {
      find(tokenId) map {
        _.map(_.deserialize[Token] ->- (tokenCache.put(tokenId, _)))
      }
    }
  }

  def findDeleted(tokenId: String) = database {
    selectOne().from(deletedTokensCollection).where("tokenId" === tokenId)
  }

  def listChildren(parent: Token): Future[List[Token]] = {
    database {
      selectAll.from(tokensCollection).where {
        ("parentTokenId" === parent.tokenId) &&
        ("tokenId"       !== parent.tokenId) 
      }
    } map { result =>
      result.toList.map(_.deserialize[Token])
    }
  }

  /** Issue a new token from the specified token.
   */
  def issueNew(parent: Token, path: Path, permissions: Permissions, expires: DateTime, limits: Limits): Future[Validation[String, Token]] = {
    if (parent.canShare) {
      val newToken = if (parent == Token.Root) {
        // This is the root token being used to create a new account:
        Token.newAccount(path, limits, permissions, expires)
      } else {
        // This is a customer token being used to create a child token:
        parent.issue(path, permissions, expires, limits)
      }

      val tokenJ = newToken.serialize.asInstanceOf[JObject]
      database(insert(tokenJ).into(tokensCollection)) map (_ => newToken.success)
    } else {
      Future.sync(("Token " + parent + " does not allow creation of child tokens.").fail)
    }
  }

  protected def deleteToken(token: Token) = for {
    _ <- database(insert(token.serialize.asInstanceOf[JObject]).into(deletedTokensCollection))
    _ <- database(remove.from(tokensCollection).where("tokenId" === token.tokenId))
  } yield {
    tokenCache.remove(token.tokenId).getOrElse(token)
  } 
}
