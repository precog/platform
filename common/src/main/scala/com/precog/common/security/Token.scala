package com.precog.common
package security

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization.{DateTimeDecomposer => _, DateTimeExtractor => _, _}
import blueeyes.json.xschema.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz._
import Scalaz._

case class Token(name: String, tid: TokenID, cid: TokenID, grants: Set[GrantID]) {
  def addGrants(add: Set[GrantID]): Token = 
    copy(grants = grants ++ add)
  def removeGrants(remove: Set[GrantID]): Token =
    copy(grants = grants -- remove)
}

trait TokenSerialization {
  implicit val TokenDecomposer: Decomposer[Token] = new Decomposer[Token] {
    override def decompose(t: Token): JValue = JObject(List(
      JField("name", t.name),
      JField("tid", t.tid),
      JField("gids", t.grants.serialize)
    )) 
  }

  val UnsafeTokenDecomposer: Decomposer[Token] = new Decomposer[Token] {
    override def decompose(t: Token): JValue = JObject(List(
      JField("name", t.name),
      JField("tid", t.tid),
      JField("cid", t.cid),
      JField("gids", t.grants.serialize)
    )) 
  }

  implicit val TokenExtractor: Extractor[Token] = new Extractor[Token] with ValidatedExtraction[Token] {    
    override def validated(obj: JValue): Validation[Error, Token] = 
      (((obj \ "name").validated[TokenID] <+> Success("(unnamed)")) |@|
       (obj \ "tid").validated[TokenID] |@|
       (obj \ "cid").validated[String] |@|
       (obj \ "gids").validated[Set[GrantID]]).apply(Token.apply _)
  }
}

object Token extends TokenSerialization

case class Grant(gid: GrantID, issuer: Option[GrantID], permission: Permission)

trait GrantSerialization {
  implicit val GrantDecomposer: Decomposer[Grant] = new Decomposer[Grant] {
    override def decompose(g: Grant): JValue = JObject(List(
      Some(JField("gid", g.gid)),
      g.issuer.map { issuer => JField("issuer", issuer.serialize) },
      Some(JField("permission", g.permission.serialize(Permission.PermissionDecomposer)))
    ).flatten)
  }

  implicit val GrantExtractor: Extractor[Grant] = new Extractor[Grant] with ValidatedExtraction[Grant] {    
    override def validated(obj: JValue): Validation[Error, Grant] = 
      ((obj \ "gid").validated[GrantID] |@|
       (obj \ "issuer").validated[Option[GrantID]] |@|
       (obj \ "permission").validated[Permission]).apply(Grant(_,_,_))
  }
}

object Grant extends GrantSerialization 


trait Permission {
  def accessType: AccessType
  def path: Path
  def expiration: Option[DateTime]

  def isExpired(ref: DateTime) = expiration.map { ref.isAfter(_) }.getOrElse(false)

}

trait PermissionSerialization {
  implicit val PermissionDecomposer: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = p match { 
      case p @ WritePermission(_, _) => p.serialize(WritePermission.WritePermissionDecomposer)
      case p @ OwnerPermission(_, _) => p.serialize(OwnerPermission.OwnerPermissionDecomposer)
      case p @ ReadPermission(_, _, _) => p.serialize(ReadPermission.ReadPermissionDecomposer)
      case p @ ReducePermission(_, _, _) => p.serialize(ReducePermission.ReducePermissionDecomposer)
    }
  }

  implicit val PermissionExtractor: Extractor[Permission] = new Extractor[Permission] with ValidatedExtraction[Permission] {    
    override def validated(obj: JValue): Validation[Error, Permission] = 
      (obj \ "type").validated[String] match {
        case Success(t) => t match {
          case WritePermission.name => obj.validated[WritePermission]
          case OwnerPermission.name => obj.validated[OwnerPermission]
          case ReadPermission.name => obj.validated[ReadPermission]
          case ReducePermission.name => obj.validated[ReducePermission]
          case _ => Failure(Invalid("Unknown permission type: " + t))
        }
        case Failure(e) => Failure(e)
      }
  }
}

object Permission extends PermissionSerialization {
  val ALL = Set[AccessType](WritePermission, OwnerPermission, ReadPermission, ReducePermission)
  val RRT = Set[AccessType](ReadPermission, ReducePermission)
  
  def permissions(
      path: Path, 
      owner: TokenID, 
      expiration: Option[DateTime], 
      grantTypes: Set[AccessType]): Set[Permission] = grantTypes.map {
    case WritePermission => WritePermission(path, expiration)
    case OwnerPermission => OwnerPermission(path, expiration)
    case ReadPermission => ReadPermission(path, owner, expiration)
    case ReducePermission => ReducePermission(path, owner, expiration)
  }

  def unapply(permission: Permission) = {
    val owner = permission match {
      case withOwner : OwnerAwarePermission => Some(withOwner.owner)
      case _ => None
    }
    Some((permission.accessType, permission.path, owner, permission.expiration))
  }
  
  def normalizeExpiration(od: Option[DateTime]): Option[DateTime] = od.flatMap { d => if (d.getMillis == 0) None else od }
}

sealed trait OwnerIgnorantPermission extends Permission {
  def derive(path: Path = path, expiration: Option[DateTime] = expiration): Permission
}

sealed trait OwnerAwarePermission extends Permission {
  def owner: TokenID 
  
  def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration): Permission
}

sealed trait AccessType {
  def name: String
}

object AccessType {
  val knownAccessTypes: Map[String, AccessType] = List(
    WritePermission, OwnerPermission,
    ReadPermission, ReducePermission
  ).map { at => (at.name, at) }(collection.breakOut)
  
  def fromString(s: String): Option[AccessType] = knownAccessTypes.get(s)
}


case class WritePermission(path: Path, expiration: Option[DateTime]) extends OwnerIgnorantPermission { 
  val accessType = WritePermission
  def derive(path: Path = path, expiration: Option[DateTime] = expiration) =
    copy(path = path, expiration = expiration)
}

trait WritePermissionSerialization {
  implicit val WritePermissionDecomposer: Decomposer[WritePermission] = new Decomposer[WritePermission] {
    override def decompose(g: WritePermission): JValue = JObject(List(
      JField("type", WritePermission.name),
      JField("path", g.path),
      JField("expirationDate", g.expiration.serialize)
    )) 
  }

  implicit val WritePermissionExtractor: Extractor[WritePermission] = new Extractor[WritePermission] with ValidatedExtraction[WritePermission] {    
    override def validated(obj: JValue): Validation[Error, WritePermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "expirationDate").validated[Option[DateTime]]).apply((p, d) => WritePermission(p,Permission.normalizeExpiration(d)))
  }
}

object WritePermission extends AccessType with WritePermissionSerialization {
  val name = "write" 
  override def toString = "WritePermission"
}


case class OwnerPermission(path: Path, expiration: Option[DateTime]) extends OwnerIgnorantPermission {
  val accessType = OwnerPermission
  def derive(path: Path = path, expiration: Option[DateTime] = expiration) =
    copy(path = path, expiration = expiration)
}

trait OwnerPermissionSerialization {
  implicit val OwnerPermissionDecomposer: Decomposer[OwnerPermission] = new Decomposer[OwnerPermission] {
    override def decompose(g: OwnerPermission): JValue = JObject(List(
      JField("type", OwnerPermission.name),
      JField("path", g.path),
      JField("expirationDate", g.expiration.serialize)
    )) 
  }

  implicit val OwnerPermissionExtractor: Extractor[OwnerPermission] = new Extractor[OwnerPermission] with ValidatedExtraction[OwnerPermission] {    
    override def validated(obj: JValue): Validation[Error, OwnerPermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "expirationDate").validated[Option[DateTime]]).apply((p, d) => OwnerPermission(p,Permission.normalizeExpiration(d)))
  }
}

object OwnerPermission extends AccessType with OwnerPermissionSerialization {
  val name = "owner"
}


case class ReadPermission(path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwarePermission {
  val accessType = ReadPermission
  def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
    copy(path = path, owner = owner, expiration = expiration)
}

trait ReadPermissionSerialization {
  implicit val ReadPermissionDecomposer: Decomposer[ReadPermission] = new Decomposer[ReadPermission] {
    override def decompose(g: ReadPermission): JValue = JObject(List(
      JField("type", ReadPermission.name),
      JField("path", g.path),
      JField("ownerAccountId", g.owner),
      JField("expirationDate", g.expiration.serialize)
    )) 
  }

  implicit val ReadPermissionExtractor: Extractor[ReadPermission] = new Extractor[ReadPermission] with ValidatedExtraction[ReadPermission] {    
    override def validated(obj: JValue): Validation[Error, ReadPermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "ownerAccountId").validated[TokenID] |@|
       (obj \ "expirationDate").validated[Option[DateTime]]).apply((p, o, d) => ReadPermission(p,o,Permission.normalizeExpiration(d)))
  }
}

object ReadPermission extends AccessType with ReadPermissionSerialization {
  val name = "read"
  override def toString = "ReadPermission"
}


case class ReducePermission(path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwarePermission {
  val accessType = ReducePermission
  def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
    copy(path = path, owner = owner, expiration = expiration)
}

trait ReducePermissionSerialization {
  implicit val ReducePermissionDecomposer: Decomposer[ReducePermission] = new Decomposer[ReducePermission] {
    override def decompose(g: ReducePermission): JValue = JObject(List(
      JField("type", ReducePermission.name),
      JField("path", g.path),
      JField("ownerAccountId", g.owner),
      JField("expirationDate", g.expiration.serialize)
    )) 
  }

  implicit val ReducePermissionExtractor: Extractor[ReducePermission] = new Extractor[ReducePermission] with ValidatedExtraction[ReducePermission] {    
    override def validated(obj: JValue): Validation[Error, ReducePermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "ownerAccountId").validated[TokenID] |@|
       (obj \ "expirationDate").validated[Option[DateTime]]).apply((p, o, d) => ReducePermission(p,o,Permission.normalizeExpiration(d)))
  }
}

object ReducePermission extends AccessType with ReducePermissionSerialization {
  val name = "reduce"
}

// vim: set ts=4 sw=4 et:
