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

case class Token(tid: TokenID, name: String, grants: Set[GrantID]) {
  def addGrants(add: Set[GrantID]): Token = 
    copy(grants = grants ++ add)
  def removeGrants(remove: Set[GrantID]): Token =
    copy(grants = grants -- remove)
}

trait TokenSerialization {
  val UnsafeTokenDecomposer: Decomposer[Token] = new Decomposer[Token] {
    override def decompose(t: Token): JValue = JObject(List(
      JField("name", t.name),
      JField("tid", t.tid),
      JField("gids", t.grants.serialize)
    )) 
  }

  implicit val SafeTokenDecomposer: Decomposer[Token] = new Decomposer[Token] {
    override def decompose(t: Token): JValue = JObject(List(
      JField("name", t.name),
      JField("tid_prefix", safeTokenID(t.tid)),
      JField("gid_prefixes", t.grants.map{ safeGrantID }.serialize)
    )) 
  }

  implicit val TokenExtractor: Extractor[Token] = new Extractor[Token] with ValidatedExtraction[Token] {    
    override def validated(obj: JValue): Validation[Error, Token] = 
      ((obj \ "tid").validated[TokenID] |@|
       (obj \ "name").validated[String] |@|
       (obj \ "gids").validated[Set[GrantID]]).apply(Token(_,_,_))
  }
}

object Token extends TokenSerialization


case class Grant(gid: GrantID, issuer: Option[GrantID], permission: Permission)

trait GrantSerialization {
  val UnsafeGrantDecomposer: Decomposer[Grant] = new Decomposer[Grant] {
    override def decompose(g: Grant): JValue = JObject(List(
      Some(JField("gid", g.gid)),
      g.issuer.map { issuer => JField("issuer", issuer.serialize) },
      Some(JField("permission", g.permission.serialize(Permission.UnsafePermissionDecomposer)))
    ).flatten)
  }

  implicit val SafeGrantDecomposer: Decomposer[Grant] = new Decomposer[Grant] {
    override def decompose(g: Grant): JValue = JObject(List(
      JField("gid_prefix", safeGrantID(g.gid)),
      JField("issuer_prefix", g.issuer.map { safeGrantID }.serialize),
      JField("permission", g.permission.serialize)
    ))
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
  val UnsafePermissionDecomposer: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = p match { 
      case p @ WritePermission(_, _) => p.serialize(WritePermission.UnsafeWritePermissionDecomposer)
      case p @ OwnerPermission(_, _) => p.serialize(OwnerPermission.UnsafeOwnerPermissionDecomposer)
      case p @ ReadPermission(_, _, _) => p.serialize(ReadPermission.UnsafeReadPermissionDecomposer)
      case p @ ReducePermission(_, _, _) => p.serialize(ReducePermission.UnsafeReducePermissionDecomposer)
    }
  }

  implicit val SafePermissionDecomposer: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = p match { 
      case p @ WritePermission(_, _) => p.serialize(WritePermission.SafeWritePermissionDecomposer)
      case p @ OwnerPermission(_, _) => p.serialize(OwnerPermission.SafeOwnerPermissionDecomposer)
      case p @ ReadPermission(_, _, _) => p.serialize(ReadPermission.SafeReadPermissionDecomposer)
      case p @ ReducePermission(_, _, _) => p.serialize(ReducePermission.SafeReducePermissionDecomposer)
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

  val UnsafeWritePermissionDecomposer: Decomposer[WritePermission] = new Decomposer[WritePermission] {
    override def decompose(g: WritePermission): JValue = JObject(List(
      JField("type", WritePermission.name),
      JField("path", g.path),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val SafeWritePermissionDecomposer: Decomposer[WritePermission] = new Decomposer[WritePermission] {
    override def decompose(g: WritePermission): JValue = JObject(List(
      JField("type", WritePermission.name),
      JField("path", g.path),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val WritePermissionExtractor: Extractor[WritePermission] = new Extractor[WritePermission] with ValidatedExtraction[WritePermission] {    
    override def validated(obj: JValue): Validation[Error, WritePermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "expiration").validated[Option[DateTime]]).apply(WritePermission(_,_))
  }
}

object WritePermission extends AccessType with WritePermissionSerialization {
  val name = "write_grant" 
  override def toString = "WritePermission"
}

case class OwnerPermission(path: Path, expiration: Option[DateTime]) extends OwnerIgnorantPermission {
  val accessType = OwnerPermission
  def derive(path: Path = path, expiration: Option[DateTime] = expiration) =
    copy(path = path, expiration = expiration)
}

trait OwnerPermissionSerialization {
  
  val UnsafeOwnerPermissionDecomposer: Decomposer[OwnerPermission] = new Decomposer[OwnerPermission] {
    override def decompose(g: OwnerPermission): JValue = JObject(List(
      JField("type", OwnerPermission.name),
      JField("path", g.path),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val SafeOwnerPermissionDecomposer: Decomposer[OwnerPermission] = new Decomposer[OwnerPermission] {
    override def decompose(g: OwnerPermission): JValue = JObject(List(
      JField("type", OwnerPermission.name),
      JField("path", g.path),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val OwnerPermissionExtractor: Extractor[OwnerPermission] = new Extractor[OwnerPermission] with ValidatedExtraction[OwnerPermission] {    
    override def validated(obj: JValue): Validation[Error, OwnerPermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "expiration").validated[Option[DateTime]]).apply(OwnerPermission(_,_))
  }

}

object OwnerPermission extends AccessType with OwnerPermissionSerialization {
  val name = "owner_grant"
}

case class ReadPermission(path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwarePermission {
  val accessType = ReadPermission
  def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
    copy(path = path, owner = owner, expiration = expiration)
}

trait ReadPermissionSerialization {
  
  val UnsafeReadPermissionDecomposer: Decomposer[ReadPermission] = new Decomposer[ReadPermission] {
    override def decompose(g: ReadPermission): JValue = JObject(List(
      JField("type", ReadPermission.name),
      JField("path", g.path),
      JField("owner", g.owner),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val SafeReadPermissionDecomposer: Decomposer[ReadPermission] = new Decomposer[ReadPermission] {
    override def decompose(g: ReadPermission): JValue = JObject(List(
      JField("type", ReadPermission.name),
      JField("path", g.path),
      JField("owner_prefix", safeTokenID(g.owner)),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val ReadPermissionExtractor: Extractor[ReadPermission] = new Extractor[ReadPermission] with ValidatedExtraction[ReadPermission] {    
    override def validated(obj: JValue): Validation[Error, ReadPermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "owner").validated[TokenID] |@|
       (obj \ "expiration").validated[Option[DateTime]]).apply(ReadPermission(_,_,_))
  }

}

object ReadPermission extends AccessType with ReadPermissionSerialization {
  val name = "read_grant"
  override def toString = "ReadPermission"
}

case class ReducePermission(path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwarePermission {
  val accessType = ReducePermission
  def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
    copy(path = path, owner = owner, expiration = expiration)
}

trait ReducePermissionSerialization {
  
  val UnsafeReducePermissionDecomposer: Decomposer[ReducePermission] = new Decomposer[ReducePermission] {
    override def decompose(g: ReducePermission): JValue = JObject(List(
      JField("type", ReducePermission.name),
      JField("path", g.path),
      JField("owner", g.owner),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val SafeReducePermissionDecomposer: Decomposer[ReducePermission] = new Decomposer[ReducePermission] {
    override def decompose(g: ReducePermission): JValue = JObject(List(
      JField("type", ReducePermission.name),
      JField("path", g.path),
      JField("owner_prefix", safeTokenID(g.owner)),
      JField("expiration", g.expiration.serialize)
    )) 
  }

  implicit val ReducePermissionExtractor: Extractor[ReducePermission] = new Extractor[ReducePermission] with ValidatedExtraction[ReducePermission] {    
    override def validated(obj: JValue): Validation[Error, ReducePermission] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "owner").validated[TokenID] |@|
       (obj \ "expiration").validated[Option[DateTime]]).apply(ReducePermission(_,_,_))
  }

}

object ReducePermission extends AccessType with ReducePermissionSerialization {
  val name = "reduce_grant"
}

// vim: set ts=4 sw=4 et:
