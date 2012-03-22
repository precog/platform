package com.precog.common

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz._
import Scalaz._

package object security {
  
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
          JField("ownerAndDescendants", JString(owner))
        ))
      }
    }

    implicit val OwnerRestrictionExtractor: Extractor[OwnerRestriction] = new Extractor[OwnerRestriction] with ValidatedExtraction[OwnerRestriction] {    
      override def validated(obj: JValue): Validation[Error, OwnerRestriction] = obj match {
        case JObject(JField("ownerAndDescendants", JString(o)) :: Nil) => Success(OwnerAndDescendants(o))
        case _                                             => Failure(Invalid("Unable to parse owner restriction."))
      }
    }
  }

  object OwnerRestriction extends OwnerRestrictionSerialization

  //case class AnyOwner extends OwnerRestriction
  case class OwnerAndDescendants(owner: UID) extends OwnerRestriction

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
        JField("pathSpec", mayAccessData.pathSpec),
        JField("ownershipSpec", mayAccessData.ownershipSpec),
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
