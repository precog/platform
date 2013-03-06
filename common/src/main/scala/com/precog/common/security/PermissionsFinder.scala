package com.precog.common
package security

import service.v1
import accounts.AccountId
import accounts.AccountFinder
import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime
import org.joda.time.Instant

import scalaz._
import scalaz.\/._
import scalaz.std.option.optionInstance
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.bitraverse._
import scalaz.syntax.std.option._

class PermissionsFinder[M[+_]: Monad](apiKeyFinder: APIKeyFinder[M], accountFinder: AccountFinder[M], timestampRequiredAfter: Instant) {
  import Permission._

  private def filterWritePermissions(keyDetails: v1.APIKeyDetails, path: Path, at: Option[Instant]): Set[WritePermission] = {
    keyDetails.grants filter { g =>
      at map { timestamp =>
        g.isValidAt(timestamp)
      } getOrElse {
        g.createdAt.isBefore(timestampRequiredAfter)
      }
    } flatMap {
      _.permissions collect { 
        case perm @ WritePermission(path0, _) if path0.isEqualOrParent(path) => perm
      }
    }
  }

  def inferWriteAuthorities(apiKey: APIKey, path: Path, at: Option[Instant]): M[Option[Authorities]] = {
    import Permission._

    def selectWriter(writePermissions: Set[WritePermission]): M[Option[Authorities]] = {
      lazy val accountWriter: M[Option[Authorities]] = accountFinder.findAccountByAPIKey(apiKey) map { _ map { Authorities(_) } }
      val eithers: Set[M[Option[Authorities]] \/ M[Option[Authorities]]] = writePermissions map {
        case WritePermission(_, WriteAsAny) => 
          left(accountWriter)
        case WritePermission(_, WriteAsAll(accountIds)) => 
          (Authorities.ifPresent(accountIds).map(a => Some(a).point[M]) \/> accountWriter)
      }

      // if it is possible to write as the account holder for the api key, then do so, otherwise,
      // write as the distinct set of writers that the api key has path write permissions for
      eithers.map(_.bisequence[M, Option[Authorities], Option[Authorities]]).sequence map { (perms: Set[Option[Authorities] \/ Option[Authorities]]) =>
        perms collectFirst {
          case -\/(Some(authorities)) => authorities
        } orElse {
          val allOptions = perms collect { case \/-(Some(authorities)) => authorities } 
          if (allOptions.size == 1) allOptions.headOption else None
        }
      }
    }

    apiKeyFinder.findAPIKey(apiKey, None) flatMap {
      _ map { (filterWritePermissions(_:v1.APIKeyDetails, path, at)) andThen (selectWriter _) } getOrElse { None.point[M] }
    }
  }

  def checkWriteAuthorities(authorities: Authorities, apiKey: APIKey, path: Path, at: Instant): M[Boolean] = {
    apiKeyFinder.findAPIKey(apiKey, None) map {
      _ map { details => 
        val permWriteAs = filterWritePermissions(details, path, Some(at)).map(_.writeAs)
        permWriteAs.exists(_ == WriteAsAny) || 
        (permWriteAs.collect({ case WriteAsAll(s) if s.subsetOf(authorities.accountIds) => s }).foldLeft(authorities.accountIds) {
          case (remaining, s) => remaining diff s
        }.isEmpty)
      } getOrElse { 
        false
      }
    }
  }
}
