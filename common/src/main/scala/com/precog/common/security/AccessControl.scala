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

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Timeout

import blueeyes._
import blueeyes.bkka.AkkaDefaults
import blueeyes.json.JPath
import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

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

class TokenManagerAccessControl(tokens: TokenManager)(implicit execContext: ExecutionContext) extends AccessControl with Logging {

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean] = 
    pathAccess match {
      case PathRead => mayAccess(uid, path, Set(uid), ReadPermission) 
      case PathWrite => mayAccess(uid, path, Set.empty, WritePermission)
    }

  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean] = 
    mayAccess(uid, path, owners, ReadPermission)
 
  def mayAccess(uid: TokenID, path: Path, owners: Set[UID], accessType: AccessType): Future[Boolean] = {
    tokens.findToken(uid).flatMap{ _.map { t => 
      logger.debug("Checking %s access to %s from token %s with owners: %s".format(accessType, path, uid, owners))
       hasValidPermissions(t, path, owners, accessType)
    }.getOrElse(Future(false)) }
  }

  def hasValidPermissions(t: Token, path: Path, owners: Set[UID], accessType: AccessType): Future[Boolean] = {

    def exists(fs: Iterable[Future[Boolean]]): Future[Boolean] = {
      if(fs.size == 0) Future(false)
      else Future.reduce(fs){ case (a,b) => a || b }
    }
    
    def forall(fs: Iterable[Future[Boolean]]): Future[Boolean] = {
      if(fs.size == 0) Future(false)
      else Future.reduce(fs){ case (a,b) => a && b }
    }

    accessType match {
      case WritePermission =>
        exists(t.grants.map{ gid =>
          tokens.findGrant(gid).flatMap( _.map { 
            case g @ Grant(_, _, WritePermission(p, _)) =>
              isValid(g).map { _ && p.equalOrChild(path) }
            case _ => Future(false)
          }.getOrElse(Future(false))
        )})
      case OwnerPermission =>
        exists(t.grants.map{ gid =>
          tokens.findGrant(gid).flatMap( _.map { 
            case g @ Grant(_, _, OwnerPermission(p, _)) =>
              isValid(g).map { _ && p.equalOrChild(path) }
            case _ => Future(false)
          }.getOrElse(Future(false))
        )})
      case ReadPermission =>
        if(owners.isEmpty) { logger.debug("Empty owners == no read permission"); Future(false) }
        else forall(owners.map { owner =>
          exists(t.grants.map{ gid =>
            tokens.findGrant(gid).flatMap( _.map {
              case g @ Grant(_, _, ReadPermission(p, o, _)) =>
                isValid(g).map { valid =>
                  val equalOrChild = p.equalOrChild(path)
                  val goodOwnership = owner == o 
                  logger.debug("Got grant %s > valid: %s, equalOrChild: %s, goodOwnership: %s".format(gid.take(10) + "...", valid, equalOrChild, goodOwnership))
                  valid && equalOrChild && goodOwnership
                }
              case _ => Future(false)
            }.getOrElse { logger.debug("Could not locate grant " + gid); Future(false) }
          )})
        })
      case ReducePermission =>
        if(owners.isEmpty) Future(false)
        else forall( owners.map { owner =>
          exists( t.grants.map{ gid =>
            tokens.findGrant(gid).flatMap( _.map { 
              case g @ Grant(_, _, ReducePermission(p, o, _)) =>
                isValid(g).map { _ && p.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
      case ModifyPermission =>
        if(owners.isEmpty) Future(false)
        else forall(owners.map { owner =>
          exists(t.grants.map { gid =>
            tokens.findGrant(gid).flatMap( _.map {
              case g @ Grant(_, _, ModifyPermission(p, o, _)) =>
                isValid(g).map { _ && p.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
      case TransformPermission =>
        if(owners.isEmpty) Future(false)
        else forall(owners.map { owner =>
          exists(t.grants.map { gid =>
            tokens.findGrant(gid).flatMap( _.map { 
              case g @ Grant(_, _, TransformPermission(p, o, _)) =>
                isValid(g).map { _ && p.equalOrChild(path) && owner == o }
              case _ => Future(false)
            }.getOrElse(Future(false))
          )})
        })
    }
  }

  def isValid(grant: Grant): Future[Boolean] = {
    (grant.issuer.map {
      tokens.findGrant(_).flatMap { _.map { parentGrant => 
        isValid(parentGrant).map { _ && grant.permission.accessType == parentGrant.permission.accessType }
      }.getOrElse { logger.warn("Could not locate issuer for grant: " + grant); Future(false) } } 
    }.getOrElse { logger.debug("No issuer, parent grant == true"); Future(true) }).map { _ && !grant.permission.isExpired(new DateTime()) }
  }
}

