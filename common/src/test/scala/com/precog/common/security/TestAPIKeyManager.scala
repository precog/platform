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

import blueeyes._
import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._
import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import org.joda.time.DateTime

import java.util.concurrent.TimeUnit._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scala.collection.mutable

import scalaz._
import scalaz.Id._
import scalaz.Validation._


class InMemoryAPIKeyManager[M[+_]: Monad](apiKeys: mutable.Map[APIKey, APIKeyRecord] = mutable.Map.empty, 
                           grants: mutable.Map[GrantID, Grant] = mutable.Map.empty) extends APIKeyManager[M] {

  private val deletedAPIKeys = mutable.Map.empty[APIKey, APIKeyRecord]
  private val deletedGrants = mutable.Map.empty[GrantID, Grant]

  def newAPIKey(name: String, creator: APIKey, grants: Set[GrantID]) = Monad[M].point {
    val apiKey = APIKeyRecord(name, newAPIKey, creator, grants)
    apiKeys.put(apiKey.tid, apiKey)
    apiKey
  }

  def newGrant(issuer: Option[GrantID], perm: Permission) = Monad[M].point {
    val newGrant = Grant(newGrantID, issuer, perm)
    grants.put(newGrant.gid, newGrant)
    newGrant
  }

  def listAPIKeys() = Monad[M].point(apiKeys.values.toList) 
  def listGrants() = Monad[M].point( grants.values.toList)

  def findAPIKey(tid: APIKey) = Monad[M].point(apiKeys.get(tid))

  def findGrant(gid: GrantID) = Monad[M].point(grants.get(gid))
  def findGrantChildren(gid: GrantID) = Monad[M].point {
    grants.values.toSet.filter{ _.issuer.map { _ == gid }.getOrElse(false) }
  }

  def addGrants(tid: APIKey, add: Set[GrantID]) = Monad[M].point {
    apiKeys.get(tid).map { t =>
      val updated = t.addGrants(add)
      apiKeys.put(tid, updated)
      updated
    }
  }

  def listDeletedAPIKeys() = Monad[M].point {
    deletedAPIKeys.values.toList 
  }

  def listDeletedGrants() = Monad[M].point {
    deletedGrants.values.toList 
  }

  def findDeletedAPIKey(tid: APIKey) = Monad[M].point {
    deletedAPIKeys.get(tid) 
  }

  def findDeletedGrant(gid: GrantID) = Monad[M].point {
    deletedGrants.get(gid) 
  }

  def findDeletedGrantChildren(gid: GrantID) = Monad[M].point {
    deletedGrants.values.toSet.filter{ _.issuer.map { _ == gid }.getOrElse(false) }
  }

  def removeGrants(tid: APIKey, remove: Set[GrantID]) = Monad[M].point {
    apiKeys.get(tid).flatMap { t =>
      if(remove.subsetOf(t.grants)) {
        val updated = t.removeGrants(remove)
        apiKeys.put(tid, updated)
        Some(updated)
      } else None
    }
  }

  def deleteAPIKey(tid: APIKey) = Monad[M].point {
    apiKeys.get(tid).flatMap { t =>
      deletedAPIKeys.put(tid, t)
      apiKeys.remove(tid)
    }
  }
  def deleteGrant(gid: GrantID) =  Monad[M].point {
    grants.remove(gid) match {
      case Some(x) =>
        deletedGrants.put(gid, x)
        Set(x)
      case _       =>
        Set.empty
    }
  }

  def close() = Monad[M].point(())
}

object TestAPIKeyManager {
  val rootUID = "root"

  val testUID = "unittest"
  val usageUID = "usage"

  val cust1UID = "user1"
  val cust2UID = "user2"

  val expiredUID = "expired"

  def standardAccountPerms(prefix: String, issuerPrefix: Option[String], path: String, owner: APIKey, expiration: Option[DateTime]): List[Grant] = {
    val config = List[(String, (Path, Option[DateTime]) => Permission)](
      ("write", WritePermission(_, _)),
      ("owner", OwnerPermission(_, _)),
      ("read", ReadPermission(_, owner, _)),
      ("reduce", ReducePermission(_, owner, _))
    )
    
    config map {
      case (suffix, f) => Grant(prefix + "_" + suffix, issuerPrefix.map { _ + "_" + suffix }, f(Path(path), expiration))
    }
  }

  def publishPathPerms(prefix: String, issuerPrefix: Option[String], path: String, owner: APIKey, expiration: Option[DateTime]) = {
    val config = List[(String, (Path, Option[DateTime]) => Permission)](
      ("read", ReadPermission(_, owner, _)),
      ("reduce", ReducePermission(_, owner, _))
    )
    
    config map {
      case (suffix, f) => Grant(prefix + "_" + suffix, issuerPrefix.map { _ + "_" + suffix }, f(Path(path), expiration))
    }
  }

  val grantList = List(
    standardAccountPerms("root", None, "/", "root", None),
    standardAccountPerms("unittest", Some("root"), "/unittest/", "unittest", None),
    standardAccountPerms("usage", Some("root"), "/__usage_tracking__", "usage_tracking", None),
    standardAccountPerms("user1", Some("root"), "/user1", "user1", None),
    standardAccountPerms("user2", Some("root"), "/user2", "user2", None),
    standardAccountPerms("expired", Some("root"), "/expired", "expired", Some(new DateTime().minusYears(1000))),
    publishPathPerms("public", Some("root"), "/public", "public", None),
    publishPathPerms("opublic", Some("root"), "/opublic", "opublic", None)
  )

  val grants: mutable.Map[GrantID, Grant] = grantList.flatten.map { g => (g.gid -> g) }(collection.breakOut)

  val apiKeys: mutable.Map[APIKey, APIKeyRecord] = List[APIKeyRecord](
    APIKeyRecord("root", "root", "", grantList(0).map { _.gid }(collection.breakOut)),
    APIKeyRecord("unittest", "unittest", "root", grantList(1).map { _.gid }(collection.breakOut)),
    APIKeyRecord("usage", "usage", "root", grantList(2).map { _.gid }(collection.breakOut)),
    APIKeyRecord("user1", "user1", "root", (grantList(3) ++ grantList(6)).map{ _.gid}(collection.breakOut)),
    APIKeyRecord("user2", "user2", "root", (grantList(4) ++ grantList(6)).map{ _.gid}(collection.breakOut)),
    APIKeyRecord("expired", "expired", "root", (grantList(5) ++ grantList(6)).map{ _.gid}(collection.breakOut))
  ).map { t => (t.tid -> t) }(collection.breakOut)
  
  val rootReadChildren = grantList.flatten.filter(_.issuer.map(_ == "root_read").getOrElse(false)).toSet

  def testAPIKeyManager[M[+_]: Monad]: APIKeyManager[M] = new InMemoryAPIKeyManager[M](apiKeys, grants)
}

// vim: set ts=4 sw=4 et:
