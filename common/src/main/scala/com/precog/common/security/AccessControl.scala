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
  def mayAccess(uid: UID, path: Path, ownders: Set[UID], accessType: AccessType): Future[Boolean]
}

class UnlimitedAccessControl(implicit executionContext: ExecutionContext) extends AccessControl {
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess) = Future(true)
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess) = Future(true)
  def mayAccess(uid: UID, path: Path, ownders: Set[UID], accessType: AccessType) = Future(true)
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

