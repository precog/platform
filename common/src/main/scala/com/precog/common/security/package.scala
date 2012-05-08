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
  private val tidSafePrefix = 30 
  private val gidSafePrefix = 30 

  private def safeTokenID(tid: TokenID): String = safePrefix(tid, tidSafePrefix) 
  private def safeGrantID(gid: GrantID): String = safePrefix(gid, gidSafePrefix) 

  private def safePrefix(s: String, prefix: Int): String = s.substring(0, math.min(s.length-1, prefix))

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

  case class ResolvedGrant(gid: GrantID, grant: Grant)
  
  trait ResolvedGrantSerialization {

    val UnsafeResolvedGrantDecomposer: Decomposer[ResolvedGrant] = new Decomposer[ResolvedGrant] {
      override def decompose(g: ResolvedGrant): JValue = JObject(List(
        JField("gid", g.gid),
        JField("grant", g.grant.serialize(Grant.UnsafeGrantDecomposer))
      )) 
    }

    implicit val SafeResolvedGrantDecomposer: Decomposer[ResolvedGrant] = new Decomposer[ResolvedGrant] {
      override def decompose(g: ResolvedGrant): JValue = JObject(List(
        JField("gid_prefix", safeGrantID(g.gid)),
        JField("grant", g.grant.serialize)
      )) 
    }

    implicit val ResolvedGrantExtractor: Extractor[ResolvedGrant] = new Extractor[ResolvedGrant] with ValidatedExtraction[ResolvedGrant] {    
      override def validated(obj: JValue): Validation[Error, ResolvedGrant] = 
        ((obj \ "gid").validated[GrantID] |@|
         (obj \ "grant").validated[Grant]).apply(ResolvedGrant(_,_))
    }
  }

  object ResolvedGrant extends ResolvedGrantSerialization with ((GrantID, Grant) => ResolvedGrant)

  sealed trait Grant {
    def accessType: AccessType
    def issuer: Option[GrantID]
    def path: Path
    def expiration: Option[DateTime]
  
    def isExpired(ref: DateTime) = expiration.map { ref.isAfter(_) }.getOrElse(false)
  }
  
  trait GrantSerialization {

    val UnsafeGrantDecomposer: Decomposer[Grant] = new Decomposer[Grant] {
      override def decompose(g: Grant): JValue = g match { 
        case g @ WriteGrant(_, _, _) => g.serialize(WriteGrant.UnsafeWriteGrantDecomposer)
        case g @ OwnerGrant(_, _, _) => g.serialize(OwnerGrant.UnsafeOwnerGrantDecomposer)
        case g @ ReadGrant(_, _, _, _) => g.serialize(ReadGrant.UnsafeReadGrantDecomposer)
        case g @ ReduceGrant(_, _, _, _) => g.serialize(ReduceGrant.UnsafeReduceGrantDecomposer)
        case g @ ModifyGrant(_, _, _, _) => g.serialize(ModifyGrant.UnsafeModifyGrantDecomposer)
        case g @ TransformGrant(_, _, _, _) => g.serialize(TransformGrant.UnsafeTransformGrantDecomposer)
      }
    }

    implicit val SafeGrantDecomposer: Decomposer[Grant] = new Decomposer[Grant] {
      override def decompose(g: Grant): JValue = g match { 
        case g @ WriteGrant(_, _, _) => g.serialize(WriteGrant.SafeWriteGrantDecomposer)
        case g @ OwnerGrant(_, _, _) => g.serialize(OwnerGrant.SafeOwnerGrantDecomposer)
        case g @ ReadGrant(_, _, _, _) => g.serialize(ReadGrant.SafeReadGrantDecomposer)
        case g @ ReduceGrant(_, _, _, _) => g.serialize(ReduceGrant.SafeReduceGrantDecomposer)
        case g @ ModifyGrant(_, _, _, _) => g.serialize(ModifyGrant.SafeModifyGrantDecomposer)
        case g @ TransformGrant(_, _, _, _) => g.serialize(TransformGrant.SafeTransformGrantDecomposer)
      }
    }

    implicit val GrantExtractor: Extractor[Grant] = new Extractor[Grant] with ValidatedExtraction[Grant] {    
      override def validated(obj: JValue): Validation[Error, Grant] = 
        (obj \ "type").validated[String] match {
          case Success(t) => t match {
            case WriteGrant.name => obj.validated[WriteGrant]
            case OwnerGrant.name => obj.validated[OwnerGrant]
            case ReadGrant.name => obj.validated[ReadGrant]
            case ReduceGrant.name => obj.validated[ReduceGrant]
            case ModifyGrant.name => obj.validated[ModifyGrant]
            case TransformGrant.name => obj.validated[TransformGrant]
            case _ => Failure(Invalid("Unknown grant type: " + t))
          }
          case Failure(e) => Failure(e)
        }
    }
  }

  object Grant extends GrantSerialization {
  
    val ALL = Set[AccessType](WriteGrant, OwnerGrant, ReadGrant, ReduceGrant, ModifyGrant, TransformGrant)
    val RRT = Set[AccessType](ReadGrant, ReduceGrant, TransformGrant)

    def grantSet(
        issuer: Option[GrantID], 
        path: Path, 
        owner: TokenID, 
        expiration: Option[DateTime], 
        grantTypes: Set[AccessType]): Set[Grant] = grantTypes.map {
      case WriteGrant => WriteGrant(issuer, path, expiration)
      case OwnerGrant => OwnerGrant(issuer, path, expiration)
      case ReadGrant => ReadGrant(issuer, path, owner, expiration)
      case ReduceGrant => ReduceGrant(issuer, path, owner, expiration)
      case ModifyGrant => ModifyGrant(issuer, path, owner, expiration)
      case TransformGrant => TransformGrant(issuer, path, owner, expiration)
    }
  }

  sealed trait OwnerIgnorantGrant extends Grant {
    def derive(issuer: Option[GrantID], path: Path = path, expiration: Option[DateTime] = expiration): Grant
  }

  sealed trait OwnerAwareGrant extends Grant {
    def owner: TokenID
    def derive(issuer: Option[GrantID], path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration): Grant
  }

  sealed trait AccessType {
    def name: String
  }

  case class WriteGrant(issuer: Option[GrantID], path: Path, expiration: Option[DateTime]) extends OwnerIgnorantGrant { 
    val accessType = WriteGrant
    def derive(issuer: Option[GrantID], path: Path = path, expiration: Option[DateTime] = expiration) =
      copy(issuer, path, expiration)
  }

  trait WriteGrantSerialization {

    val UnsafeWriteGrantDecomposer: Decomposer[WriteGrant] = new Decomposer[WriteGrant] {
      override def decompose(g: WriteGrant): JValue = JObject(List(
        JField("type", WriteGrant.name),
        JField("issuer", g.issuer.serialize),
        JField("path", g.path),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val SafeWriteGrantDecomposer: Decomposer[WriteGrant] = new Decomposer[WriteGrant] {
      override def decompose(g: WriteGrant): JValue = JObject(List(
        JField("type", WriteGrant.name),
        JField("issuer_prefix", g.issuer.map { safeGrantID }.serialize),
        JField("path", g.path),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val WriteGrantExtractor: Extractor[WriteGrant] = new Extractor[WriteGrant] with ValidatedExtraction[WriteGrant] {    
      override def validated(obj: JValue): Validation[Error, WriteGrant] = 
        ((obj \ "issuer").validated[Option[GrantID]] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(WriteGrant(_,_,_))
    }
  }

  object WriteGrant extends AccessType with WriteGrantSerialization {
    val name = "write_grant" 
  }

  case class OwnerGrant(issuer: Option[GrantID], path: Path, expiration: Option[DateTime]) extends OwnerIgnorantGrant {
    val accessType = OwnerGrant
    def derive(issuer: Option[GrantID], path: Path = path, expiration: Option[DateTime] = expiration) =
      copy(issuer, path, expiration)
  }

  trait OwnerGrantSerialization {
    
    val UnsafeOwnerGrantDecomposer: Decomposer[OwnerGrant] = new Decomposer[OwnerGrant] {
      override def decompose(g: OwnerGrant): JValue = JObject(List(
        JField("type", OwnerGrant.name),
        JField("issuer", g.issuer.serialize),
        JField("path", g.path),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val SafeOwnerGrantDecomposer: Decomposer[OwnerGrant] = new Decomposer[OwnerGrant] {
      override def decompose(g: OwnerGrant): JValue = JObject(List(
        JField("type", OwnerGrant.name),
        JField("issuer_prefix", g.issuer.map { safeGrantID }.serialize),
        JField("path", g.path),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val OwnerGrantExtractor: Extractor[OwnerGrant] = new Extractor[OwnerGrant] with ValidatedExtraction[OwnerGrant] {    
      override def validated(obj: JValue): Validation[Error, OwnerGrant] = 
        ((obj \ "issuer").validated[Option[GrantID]] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(OwnerGrant(_,_,_))
    }

  }

  object OwnerGrant extends AccessType with OwnerGrantSerialization {
    val name = "owner_grant"
  }

  case class ReadGrant(issuer: Option[GrantID], path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwareGrant {
    val accessType = ReadGrant
    def derive(issuer: Option[GrantID], path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
      copy(issuer, path, owner, expiration.serialize)
  }

  trait ReadGrantSerialization {
    
    val UnsafeReadGrantDecomposer: Decomposer[ReadGrant] = new Decomposer[ReadGrant] {
      override def decompose(g: ReadGrant): JValue = JObject(List(
        JField("type", ReadGrant.name),
        JField("issuer", g.issuer.serialize),
        JField("path", g.path),
        JField("owner", g.owner),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val SafeReadGrantDecomposer: Decomposer[ReadGrant] = new Decomposer[ReadGrant] {
      override def decompose(g: ReadGrant): JValue = JObject(List(
        JField("type", ReadGrant.name),
        JField("issuer_prefix", g.issuer.map { safeGrantID }.serialize),
        JField("path", g.path),
        JField("owner_prefix", safeTokenID(g.owner)),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val ReadGrantExtractor: Extractor[ReadGrant] = new Extractor[ReadGrant] with ValidatedExtraction[ReadGrant] {    
      override def validated(obj: JValue): Validation[Error, ReadGrant] = 
        ((obj \ "issuer").validated[Option[GrantID]] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[TokenID] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(ReadGrant(_,_,_,_))
    }

  }

  object ReadGrant extends AccessType with ReadGrantSerialization {
    val name = "read_grant"
  }

  case class ReduceGrant(issuer: Option[GrantID], path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwareGrant {
    val accessType = ReduceGrant
    def derive(issuer: Option[GrantID], path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
      copy(issuer, path, owner, expiration)
  }
  
  trait ReduceGrantSerialization {
    
    val UnsafeReduceGrantDecomposer: Decomposer[ReduceGrant] = new Decomposer[ReduceGrant] {
      override def decompose(g: ReduceGrant): JValue = JObject(List(
        JField("type", ReduceGrant.name),
        JField("issuer", g.issuer.serialize),
        JField("path", g.path),
        JField("owner", g.owner),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val SafeReduceGrantDecomposer: Decomposer[ReduceGrant] = new Decomposer[ReduceGrant] {
      override def decompose(g: ReduceGrant): JValue = JObject(List(
        JField("type", ReduceGrant.name),
        JField("issuer_prefix", g.issuer.map { safeGrantID }.serialize),
        JField("path", g.path),
        JField("owner_prefix", safeTokenID(g.owner)),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val ReduceGrantExtractor: Extractor[ReduceGrant] = new Extractor[ReduceGrant] with ValidatedExtraction[ReduceGrant] {    
      override def validated(obj: JValue): Validation[Error, ReduceGrant] = 
        ((obj \ "issuer").validated[Option[GrantID]] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[TokenID] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(ReduceGrant(_,_,_,_))
    }

  }

  object ReduceGrant extends AccessType with ReduceGrantSerialization {
    val name = "reduce_grant"
  }

  case class ModifyGrant(issuer: Option[GrantID], path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwareGrant {
    val accessType = ReduceGrant
    def derive(issuer: Option[GrantID], path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
      copy(issuer, path, owner, expiration)
  }
  
  trait ModifyGrantSerialization {
    
    val UnsafeModifyGrantDecomposer: Decomposer[ModifyGrant] = new Decomposer[ModifyGrant] {
      override def decompose(g: ModifyGrant): JValue = JObject(List(
        JField("type", ModifyGrant.name),
        JField("issuer", g.issuer.serialize),
        JField("path", g.path),
        JField("owner", g.owner),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val SafeModifyGrantDecomposer: Decomposer[ModifyGrant] = new Decomposer[ModifyGrant] {
      override def decompose(g: ModifyGrant): JValue = JObject(List(
        JField("type", ModifyGrant.name),
        JField("issuer_prefix", g.issuer.map { safeGrantID }.serialize),
        JField("path", g.path),
        JField("owner_prefix", safeTokenID(g.owner)),
        JField("expiration", g.expiration.serialize)
      )) 
    }

    implicit val ModifyGrantExtractor: Extractor[ModifyGrant] = new Extractor[ModifyGrant] with ValidatedExtraction[ModifyGrant] {    
      override def validated(obj: JValue): Validation[Error, ModifyGrant] = 
        ((obj \ "issuer").validated[Option[GrantID]] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[TokenID] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(ModifyGrant(_,_,_,_))
    }

  }

  object ModifyGrant extends AccessType with ModifyGrantSerialization {
    val name = "modify_grant"
  }

  case class TransformGrant(issuer: Option[GrantID], path: Path, owner: TokenID, expiration: Option[DateTime]) extends OwnerAwareGrant {
    val accessType = ReduceGrant
    def derive(issuer: Option[GrantID], path: Path = path, owner: TokenID = owner, expiration: Option[DateTime] = expiration) =
      copy(issuer, path, owner, expiration)
  }
  
  trait TransformGrantSerialization {
    
    val UnsafeTransformGrantDecomposer: Decomposer[TransformGrant] = new Decomposer[TransformGrant] {
      override def decompose(g: TransformGrant): JValue = JObject(List(
        JField("type", TransformGrant.name),
        JField("issuer", g.issuer.serialize),
        JField("path", g.path),
        JField("owner", g.owner),
        JField("expiration", g.expiration)
      )) 
    }

    implicit val SafeTransformGrantDecomposer: Decomposer[TransformGrant] = new Decomposer[TransformGrant] {
      override def decompose(g: TransformGrant): JValue = JObject(List(
        JField("type", TransformGrant.name),
        JField("issuer_prefix", g.issuer.map { safeGrantID(_) }.serialize),
        JField("path", g.path),
        JField("owner_prefix", safeTokenID(g.owner)),
        JField("expiration", g.expiration)
      )) 
    }

    implicit val TransformGrantExtractor: Extractor[TransformGrant] = new Extractor[TransformGrant] with ValidatedExtraction[TransformGrant] {    
      override def validated(obj: JValue): Validation[Error, TransformGrant] = 
        ((obj \ "issuer").validated[Option[GrantID]] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[TokenID] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(TransformGrant(_,_,_,_))
    }

  }

  object TransformGrant extends AccessType with TransformGrantSerialization {
    val name = "transform_grant"
  }

  // legacy security features held over until complete token/grant refactor complete

  type UID = String

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

package object osecurity {
  
  type UID = String

  sealed trait PathRestriction {
    def matches(path: Path): Boolean
  }

  trait PathRestrictionSerialization {
    implicit val PathRestrictionDecomposer: Decomposer[PathRestriction] = new Decomposer[PathRestriction] {
      override def decompose(pathSpec: PathRestriction): JValue = pathSpec match {
        case Subtree(path: Path) => JObject(List(
          JField("subtree", JString(path.toString))
        ))
      }
    }

    implicit val PathRestrictionExtractor: Extractor[PathRestriction] = new Extractor[PathRestriction] with ValidatedExtraction[PathRestriction] {    
      override def validated(obj: JValue): Validation[Error, PathRestriction] = obj match {
        case JObject(JField("subtree", JString(p)) :: Nil) => Success(Subtree(Path(p)))
        case _                                             => Failure(Invalid("Unable to parse path restriction."))
      }
    }
  }

  object PathRestriction extends PathRestrictionSerialization
  
  //case object AnyPath extends PathRestriction
  case class Subtree(path: Path) extends PathRestriction {
    def matches(test: Path) = { 
      path.equalOrChild(test)
    }   
  }
 
  sealed trait OwnerRestriction

  trait OwnerRestrictionSerialization {
    implicit val OwnerRestrictionDecomposer: Decomposer[OwnerRestriction] = new Decomposer[OwnerRestriction] {
      override def decompose(ownerSpec: OwnerRestriction): JValue = ownerSpec match {
        case OwnerAndDescendants(owner) => JObject(List(
          JField("ownerRestriction", JString(owner))

        ))
        case HolderAndDescendants => JObject(List(
          JField("ownerRestriction", JString("[HOLDER]"))
        ))
      }
    }

    implicit val OwnerRestrictionExtractor: Extractor[OwnerRestriction] = new Extractor[OwnerRestriction] with ValidatedExtraction[OwnerRestriction] {    
      override def validated(obj: JValue): Validation[Error, OwnerRestriction] = obj match {
        case JObject(JField("ownerRestriction", JString(o)) :: Nil) => o match {
          case "[HOLDER]" => Success(HolderAndDescendants)
          case o   => Success(OwnerAndDescendants(o))
        }
        case _                                             => Failure(Invalid("Unable to parse owner restriction."))
      }
    }
  }

  object OwnerRestriction extends OwnerRestrictionSerialization

  //case class AnyOwner extends OwnerRestriction
  case class OwnerAndDescendants(owner: UID) extends OwnerRestriction
  case object HolderAndDescendants extends OwnerRestriction

  sealed trait PathAccess {
    def symbol: String
  }
  
  trait PathAccessSerialization {
    implicit val PathAccessDecomposer: Decomposer[PathAccess] = new Decomposer[PathAccess] {
      override def decompose(pathAccess: PathAccess): JValue = JString(pathAccess.symbol)
    }

    implicit val PathAccessExtractor: Extractor[PathAccess] = new Extractor[PathAccess] with ValidatedExtraction[PathAccess] {    
      override def validated(obj: JValue): Validation[Error, PathAccess] = obj match {
        case JString(PathRead.symbol)  => Success(PathRead)
        case JString(PathWrite.symbol) => Success(PathWrite)
        case _                         => Failure(Invalid("Unable to parse path access value."))
      }
    }
  }

  object PathAccess extends PathAccessSerialization

  case object PathRead extends PathAccess {
    val symbol = "PATH_READ"
  }

  case object PathWrite extends PathAccess {
    val symbol = "PATH_WRITE"
  }

  sealed trait DataAccess {
    def symbol: String
  }
  
  trait DataAccessSerialization {
    implicit val DataAccessDecomposer: Decomposer[DataAccess] = new Decomposer[DataAccess] {
      override def decompose(dataAccess: DataAccess): JValue = JString(dataAccess.symbol)
    }

    implicit val DataAccessExtractor: Extractor[DataAccess] = new Extractor[DataAccess] with ValidatedExtraction[DataAccess] {    
      override def validated(obj: JValue): Validation[Error, DataAccess] = obj match {
        case JString(DataQuery.symbol) => Success(DataQuery)
        case _                         => Failure(Invalid("Unable to parse data access value."))
      }
    }
  }

  object DataAccess extends DataAccessSerialization

  case object DataQuery extends DataAccess {
    val symbol = "DATA_QUERY"
  }

  case class MayAccessPath(pathSpec: PathRestriction, pathAccess: PathAccess, mayShare: Boolean)

  trait MayAccessPathSerialization {
    implicit val MayAccessPathDecomposer: Decomposer[MayAccessPath] = new Decomposer[MayAccessPath] {
      override def decompose(mayAccessPath: MayAccessPath): JValue = JObject(List( 
        JField("pathSpec", mayAccessPath.pathSpec),
        JField("pathAccess", mayAccessPath.pathAccess),
        JField("mayShare", mayAccessPath.mayShare)
      ))    
    }

    implicit val MayAccessPathExtractor: Extractor[MayAccessPath] = new Extractor[MayAccessPath] with ValidatedExtraction[MayAccessPath] {    
      override def validated(obj: JValue): Validation[Error, MayAccessPath] =
        ((obj \ "pathSpec").validated[PathRestriction] |@|
         (obj \ "pathAccess").validated[PathAccess] |@|
         (obj \ "mayShare").validated[Boolean]).apply(MayAccessPath(_,_,_))
    }
  }

  object MayAccessPath extends MayAccessPathSerialization

  case class MayAccessData(pathSpec: PathRestriction, ownershipSpec: OwnerRestriction, dataAccess: DataAccess, mayShare: Boolean)
  
  trait MayAccessDataSerialization {
    implicit val MayAccessDataDecomposer: Decomposer[MayAccessData] = new Decomposer[MayAccessData] {
      override def decompose(mayAccessData: MayAccessData): JValue = JObject(List( 
        JField("pathSpec", mayAccessData.pathSpec.serialize),
        JField("ownershipSpec", mayAccessData.ownershipSpec.serialize),
        JField("dataAccess", mayAccessData.dataAccess),
        JField("mayShare", mayAccessData.mayShare)
      ))    
    }

    implicit val MayAccessDataExtractor: Extractor[MayAccessData] = new Extractor[MayAccessData] with ValidatedExtraction[MayAccessData] {    
      override def validated(obj: JValue): Validation[Error, MayAccessData] = 
        ((obj \ "pathSpec").validated[PathRestriction] |@|
         (obj \ "ownershipSpec").validated[OwnerRestriction] |@|
         (obj \ "dataAccess").validated[DataAccess] |@|
         (obj \ "mayShare").validated[Boolean]).apply(MayAccessData(_,_,_,_))
    }
  }

  object MayAccessData extends MayAccessDataSerialization
  
  case class Permissions(path: Set[MayAccessPath], data: Set[MayAccessData]) {
    def ++(that: Permissions) = Permissions(this.path ++ that.path, this.data ++ that.data)
    def sharable: Permissions = 
      Permissions( 
        this.path.filter{ _.mayShare },
        this.data.filter{ _.mayShare }
      )
  }
  
  trait PermissionsSerialization {
    implicit val PermissionsDecomposer: Decomposer[Permissions] = new Decomposer[Permissions] {
      override def decompose(perms: Permissions): JValue = JObject(List( 
        JField("path", perms.path.serialize),
        JField("data", perms.data.serialize)
      ))    
    }

    implicit val PermissionsExtractor: Extractor[Permissions] = new Extractor[Permissions] with ValidatedExtraction[Permissions] {    
      override def validated(obj: JValue): Validation[Error, Permissions] =
        ((obj \ "path").validated[Set[MayAccessPath]] |@|
         (obj \ "data").validated[Set[MayAccessData]]).apply(Permissions(_,_))
    }
  }

  object Permissions extends PermissionsSerialization {
    def apply(path: MayAccessPath*)(data: MayAccessData*): Permissions = Permissions(path.toSet, data.toSet)
    val empty = Permissions(Set.empty[MayAccessPath], Set.empty[MayAccessData])
  }

  case class Token(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean) {
    val isValid = !expired
  }

  trait TokenSerialization {
    implicit val TokenDecomposer: Decomposer[Token] = new Decomposer[Token] {
      override def decompose(token: Token): JValue = JObject(List( 
        JField("uid", token.uid),
        JField("issuer", token.issuer.serialize),
        JField("permissions", token.permissions),
        JField("grants", token.grants.serialize),
        JField("expired", token.expired)
      ))    
    }

    implicit val TokenExtractor: Extractor[Token] = new Extractor[Token] with ValidatedExtraction[Token] {    
      override def validated(obj: JValue): Validation[Error, Token] =
        ((obj \ "uid").validated[UID] |@|
         (obj \ "issuer").validated[Option[UID]] |@|
         (obj \ "permissions").validated[Permissions] |@|
         (obj \ "grants").validated[Set[UID]] |@|
         (obj \ "expired").validated[Boolean]).apply(Token(_,_,_,_,_))
    }
  }

  object Token extends TokenSerialization with ((UID, Option[UID], Permissions, Set[UID], Boolean) => Token)

}
