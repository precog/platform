package com.precog.common

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization.{DateTimeDecomposer => _, DateTimeExtractor => _, _}
import blueeyes.json.xschema.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz._
import Scalaz._

package object security {

  private val isoFormat = ISODateTimeFormat.dateTime

  implicit val TZDateTimeDecomposer: Decomposer[DateTime] = new Decomposer[DateTime] {
    override def decompose(d: DateTime): JValue = JString(isoFormat.print(d))
  }

  implicit val TZDateTimeExtractor: Extractor[DateTime] = new Extractor[DateTime] with ValidatedExtraction[DateTime] {    
    override def validated(obj: JValue): Validation[Error, DateTime] = obj match {
      case JString(dt) => Success(isoFormat.parseDateTime(dt))
      case _           => Failure(Invalid("Date time must be represented as JSON string"))
    }
  }

  implicit val OptionDateTimeDecomposer: Decomposer[Option[DateTime]] = OptionDecomposer[DateTime]
  implicit val OptionDateTimeExtractor: Extractor[Option[DateTime]] = OptionExtractor[DateTime]
   
  type TokenID = String
  type GrantID = String
  type UID = String
}

package security {

  object Util {
    val tidSafePrefix = 30 
    val gidSafePrefix = 30 

    def safeTokenID(tid: TokenID): String = safePrefix(tid, tidSafePrefix) 
    def safeGrantID(gid: GrantID): String = safePrefix(gid, gidSafePrefix) 

    def safePrefix(s: String, prefix: Int): String = s.substring(0, math.min(s.length-1, prefix))
  }

  import Util._ 

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
        JField("gid", g.gid),
        JField("issuer", g.issuer.serialize),
        JField("permission", g.permission.serialize(Permission.UnsafePermissionDecomposer))
      ))
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
        case p @ ModifyPermission(_, _, _) => p.serialize(ModifyPermission.UnsafeModifyPermissionDecomposer)
        case p @ TransformPermission(_, _, _) => p.serialize(TransformPermission.UnsafeTransformPermissionDecomposer)
      }
    }

    implicit val SafePermissionDecomposer: Decomposer[Permission] = new Decomposer[Permission] {
      override def decompose(p: Permission): JValue = p match { 
        case p @ WritePermission(_, _) => p.serialize(WritePermission.SafeWritePermissionDecomposer)
        case p @ OwnerPermission(_, _) => p.serialize(OwnerPermission.SafeOwnerPermissionDecomposer)
        case p @ ReadPermission(_, _, _) => p.serialize(ReadPermission.SafeReadPermissionDecomposer)
        case p @ ReducePermission(_, _, _) => p.serialize(ReducePermission.SafeReducePermissionDecomposer)
        case p @ ModifyPermission(_, _, _) => p.serialize(ModifyPermission.SafeModifyPermissionDecomposer)
        case p @ TransformPermission(_, _, _) => p.serialize(TransformPermission.SafeTransformPermissionDecomposer)
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
            case ModifyPermission.name => obj.validated[ModifyPermission]
            case TransformPermission.name => obj.validated[TransformPermission]
            case _ => Failure(Invalid("Unknown permission type: " + t))
          }
          case Failure(e) => Failure(e)
        }
    }
  }

  object Permission extends PermissionSerialization {
    
    val ALL = Set[AccessType](WritePermission, OwnerPermission, ReadPermission, ReducePermission, ModifyPermission, TransformPermission)
    val RRT = Set[AccessType](ReadPermission, ReducePermission, TransformPermission)
    
    def permissions(
        path: Path, 
        owner: TokenID, 
        expiration: Option[DateTime], 
        grantTypes: Set[AccessType]): Set[Permission] = grantTypes.map {
      case WritePermission => WritePermission(path, expiration)
      case OwnerPermission => OwnerPermission(path, expiration)
      case ReadPermission => ReadPermission(path, owner, expiration)
      case ReducePermission => ReducePermission(path, owner, expiration)
      case ModifyPermission => ModifyPermission(path, owner, expiration)
      case TransformPermission => TransformPermission(path, owner, expiration)
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
      ReadPermission, ReducePermission, ModifyPermission, TransformPermission
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

  case class ModifyPermission(path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwarePermission {
    val accessType = ReducePermission
    def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
      copy(path = path, owner = owner, expiration = expiration)
  }
  
  trait ModifyPermissionSerialization {
    
    val UnsafeModifyPermissionDecomposer: Decomposer[ModifyPermission] = new Decomposer[ModifyPermission] {
      override def decompose(g: ModifyPermission): JValue = JObject(List(
        JField("type", ModifyPermission.name),
        JField("path", g.path),
        JField("owner", g.owner),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val SafeModifyPermissionDecomposer: Decomposer[ModifyPermission] = new Decomposer[ModifyPermission] {
      override def decompose(g: ModifyPermission): JValue = JObject(List(
        JField("type", ModifyPermission.name),
        JField("path", g.path),
        JField("owner_prefix", safeTokenID(g.owner)),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val ModifyPermissionExtractor: Extractor[ModifyPermission] = new Extractor[ModifyPermission] with ValidatedExtraction[ModifyPermission] {    
      override def validated(obj: JValue): Validation[Error, ModifyPermission] = 
        ((obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[TokenID] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(ModifyPermission(_,_,_))
    }

  }

  object ModifyPermission extends AccessType with ModifyPermissionSerialization {
    val name = "modify_grant"
  }

  case class TransformPermission(path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwarePermission {
    val accessType = ReducePermission
    def derive(path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
      copy(path = path, owner = owner, expiration = expiration)
  }
  
  trait TransformPermissionSerialization {
    
    val UnsafeTransformPermissionDecomposer: Decomposer[TransformPermission] = new Decomposer[TransformPermission] {
      override def decompose(g: TransformPermission): JValue = JObject(List(
        JField("type", TransformPermission.name),
        JField("path", g.path),
        JField("owner", g.owner),
        JField("expiration", g.expiration)
      )) 
    }

    implicit val SafeTransformPermissionDecomposer: Decomposer[TransformPermission] = new Decomposer[TransformPermission] {
      override def decompose(g: TransformPermission): JValue = JObject(List(
        JField("type", TransformPermission.name),
        JField("path", g.path),
        JField("owner_prefix", safeTokenID(g.owner)),
        JField("expiration", g.expiration)
      )) 
    }

    implicit val TransformPermissionExtractor: Extractor[TransformPermission] = new Extractor[TransformPermission] with ValidatedExtraction[TransformPermission] {    
      override def validated(obj: JValue): Validation[Error, TransformPermission] = 
        ((obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[TokenID] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(TransformPermission(_,_,_))
    }

  }

  object TransformPermission extends AccessType with TransformPermissionSerialization {
    val name = "transform_grant"
  }

  // legacy security features held over until complete token/grant refactor complete

  sealed trait PathAccess {
    def symbol: String
  }

  case object PathRead extends PathAccess {
    val symbol = "PATH_READ"
  }

  case object PathWrite extends PathAccess {
    val symbol = "PATH_WRITE"
  }

  sealed trait DataAccess {
    def symbol: String
  }
  
  case object DataQuery extends DataAccess {
    val symbol = "DATA_QUERY"
  }

}

