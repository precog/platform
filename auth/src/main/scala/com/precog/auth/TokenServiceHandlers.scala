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
package com.precog
package auth

import com.precog.common.Path
import com.precog.common.security._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.xschema.DefaultSerialization._

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz.{Validation, Success, Failure}
import scalaz.Scalaz._

import org.joda.time.DateTime

class GetTokenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      Future(HttpResponse[JValue](OK, content = Some(t.serialize)))
    }
  }
  val metadata = None
}

class AddTokenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Token => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      request.content.map { _.flatMap { _.validated[NewTokenRequest] match {
        case Success(r) =>
          tokenManagement.newToken(t.tid, r).map { 
            case Success(r) => 
              HttpResponse[JValue](OK, content = Some(r.serialize))
            case Failure(e) => 
              HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new token."), content = Some(JObject(List(
                JField("error", "Error creating new token: " + e)
              ))))
          }
        case Failure(e) =>
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new token request body."), content = Some(JObject(List(
            JField("error", "Invalid new token request body: " + e)
          )))))
      }}}.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing new token request body."), content = Some(JString("Missing new token request body."))))
      }
    }
  }
  val metadata = None
}

class GetGrantsHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      tokenManagement.findTokenGrants(t.tid).map { grants =>
        HttpResponse[JValue](OK, content = Some(JArray(grants.map { _.serialize }(collection.breakOut)))) 
      }
    }
  }
  val metadata = None
}

class AddGrantHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("todo")))) 
    }
  }
  val metadata = None
}

class RemoveGrantHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("todo")))) 
    }
  }
  val metadata = None
}

class GetGrantChildrenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("todo")))) 
    }
  }
  val metadata = None
}

class AddGrantChildrenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("todo")))) 
    }
  }
  val metadata = None
}

class GetGrantChildHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("todo")))) 
    }
  }
  val metadata = None
}

class RemoveGrantChildHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("todo")))) 
    }
  }
  val metadata = None
}

class TokenManagement(tokenManager: TokenManager[Future]) {

  def findTokenAndGrants(tid: TokenID): Future[Option[(Token, Set[Grant])]] = sys.error("todo")

  def newToken(issuer: TokenID, request: NewTokenRequest): Future[Validation[String, Token]] = sys.error("todo")

  def findTokenGrants(tid: TokenID): Future[Set[Grant]] = sys.error("todo") 
  def addTokenGrant(tid: TokenID, gid: GrantID): Future[Validation[String, Unit]] = sys.error("todo")
  def deleteTokenGrant(tid: TokenID, gid: GrantID): Future[Validation[String, Unit]] = sys.error("todo")

  def findGrantChildren(tid: TokenID, gid: GrantID): Future[Set[Grant]] = sys.error("todo")
  def newGrantChild(tid: TokenID, gid: GrantID, request: NewGrantRequest): Future[Validation[String, Grant]] = sys.error("todo")
  def deleteGrantChild(tid: TokenID, gid: GrantID, childGid: GrantID): Future[Validation[String, Unit]] = sys.error("todo")

  def close() = tokenManager.close()
}


case class NewTokenRequest(name: String, grants: List[NewGrantRequest], readback: Boolean)

trait NewTokenRequestSerialization {
    implicit val NewTokenRequestExtractor: Extractor[NewTokenRequest] = new Extractor[NewTokenRequest] with ValidatedExtraction[NewTokenRequest] {    
      override def validated(obj: JValue): Validation[Error, NewTokenRequest] = 
        ((obj \ "name").validated[String] |@|
         (obj \ "grants").validated[List[NewGrantRequest]] |@|
         (obj \ "readback").validated[Boolean]).apply(NewTokenRequest(_,_,_))
    }
}

object NewTokenRequest extends NewTokenRequestSerialization

case class NewGrantRequest(accessTypeString: String, path: Path, owner: Option[String], expiration: Option[DateTime]) {
  val accessType = AccessType.fromString(accessTypeString) 
  def toPermission(): Validation[String, Permission] = {
    accessType match {
      case Some(WritePermission) =>
        Success(WritePermission(path, expiration))
      case Some(OwnerPermission) =>
        Success(OwnerPermission(path, expiration))
      case Some(ReadPermission) =>
        owner.map { o => Success(ReadPermission(path, o, expiration)) }.getOrElse{ Failure("Unable to create grant without owner") }
      case Some(ReducePermission) =>
        owner.map { o => Success(ReducePermission(path, o, expiration)) }.getOrElse{ Failure("Unable to create grant without owner") }
      case Some(ModifyPermission) =>
        owner.map { o => Success(ModifyPermission(path, o, expiration)) }.getOrElse{ Failure("Unable to create grant without owner") }
      case Some(TransformPermission) =>
        owner.map { o => Success(TransformPermission(path, o, expiration)) }.getOrElse{ Failure("Unable to create grant without owner") }
      case None => Failure("Unknown access type: " + accessType)
    } 
  }
}

trait NewGrantRequestSerialization {
    
    implicit val NewGrantRequestExtractor: Extractor[NewGrantRequest] = new Extractor[NewGrantRequest] with ValidatedExtraction[NewGrantRequest] {    
      override def validated(obj: JValue): Validation[Error, NewGrantRequest] = 
        ((obj \ "accessType").validated[String] |@|
         (obj \ "path").validated[Path] |@|
         (obj \ "owner").validated[Option[TokenID]] |@|
         (obj \ "expiration").validated[Option[DateTime]]).apply(NewGrantRequest(_,_,_,_))
    }

}

object NewGrantRequest extends NewGrantRequestSerialization













class CreateTokenHandler(tokenManager: TokenManager[Future], accessControl: AccessControl[Future])(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging with HandlerHelpers {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => Future(todo) }
//      request.content.map { _.flatMap { _.validated[TokenCreate] match {
//        case Success(tokenCreate) =>
//          tokenCreate.permissions.map{ Future(_) }.getOrElse{ tokenManager.sharablePermissions(t) }.flatMap{ perms =>
//            accessControl.mayGrant(t.uid, perms) flatMap {
//              case false => Future[Resp]((Unauthorized, "The specified token may not grant the requested permissions"))
//              case true  =>
//                tokenManager.issueNew(Some(t.uid), perms, 
//                                      tokenCreate.grants.getOrElse(Set()), 
//                                      tokenCreate.expired.getOrElse(false)) map { 
//                  case Success(t) => (OK, t.serialize): Resp
//                  case Failure(e) => (BadRequest, e): Resp
//                }
//            }
//          }
//        case Failure(e)           => Future[Resp](BadRequest, e)
//      }}}.getOrElse(Future[Resp]((BadRequest -> "Missing token create request body.")))
//    }
  }

  val metadata = None
}

class DeleteTokenHandler(tokenManager: TokenManager[Future])(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging with HandlerHelpers {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => Future(todo) } 
//      request.parameters.get('delete) match {
//        case None         =>
//          Future(HttpResponse[JValue](BadRequest, content=Some(JString("Token to be deleted was not found."))))
//        case Some(delete) =>
//          tokenManager.deleteDescendant(t, delete) map { deleted =>
//            val content = JArray(deleted.map{ t => JString(t.uid) })
//            HttpResponse[JValue](OK, content=Some(content)) 
//          }
//      }     
//    }
  }
  val metadata = None
}

class UpdateTokenHandler(tokenManager: TokenManager[Future])(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging with HandlerHelpers {

  val service  = (request: HttpRequest[Future[JValue]]) => {
//    request.parameters.get('update) match {
//      case None            =>
//        Failure(inapplicable)
//      case Some(updateUID) => 
        Success {(t: Token) => Future(todo)}
        //tokenManager.findToken(updateUID) flatMap {
//          case None              => Future[Resp]((BadRequest, "Token to update doesn't exists."))
//          case Some(updateToken) => request.content.map { _.flatMap { _.validated[TokenUpdate] match {
//            case Success(tokenUpdate) =>
//              val toUpdate = applyUpdates(updateToken, tokenUpdate)
//              tokenManager.updateToken(t, toUpdate) map {
//                case Success(rt) => (OK, rt.serialize): Resp
//                case Failure(e) => (BadRequest, e): Resp
//              }
//            case Failure(e)  => Future[Resp]((BadRequest, e))
//          }}}.getOrElse(Future[Resp]((BadRequest -> "Missing token update request body.")))
//        }
//      }
//    }
  }

  val metadata = None

//  def applyUpdates(t: Token, tu: TokenUpdate): Token = {
//    Token(t.uid, t.issuer, t.permissions, (t.grants ++ tu.addGrants) -- tu.removeGrants, tu.expired.getOrElse(t.expired))
//  }
}

trait HandlerHelpers {
 
  type Resp = HttpResponse[JValue]

  val todo: Resp = (BadRequest, JString("todo"))
  
  implicit def codeAndMessageToResponse(t: (HttpStatusCode, String)): HttpResponse[JValue] = HttpResponse(t._1, content = Some(JString(t._2)))
  implicit def codeAndValueToResponse(t: (HttpStatusCode, JValue)): HttpResponse[JValue] = HttpResponse(t._1, content = Some(t._2))
  implicit def codeAndErrorToResponse(t: (HttpStatusCode, Error)): HttpResponse[JValue] = HttpResponse(t._1, content = Some(t._2.message))

}

//case class TokenCreate(permissions: Option[Permissions], grants: Option[Set[UID]], expired: Option[Boolean])
//
//trait TokenCreateSerialization {
//  implicit val TokenCreateDecomposer: Decomposer[TokenCreate] = new Decomposer[TokenCreate] {
//    override def decompose(token: TokenCreate): JValue = JObject(List(  
//      JField("permissions", token.permissions.serialize),
//      JField("grants", token.grants.serialize),
//      JField("expired", token.expired.serialize)
//    ))        
//  }     
//
//  implicit val TokenCreateExtractor: Extractor[TokenCreate] = new Extractor[TokenCreate] with ValidatedExtraction[TokenCreate] {            
//    override def validated(obj: JValue): Validation[Error, TokenCreate] = {
//      val fields = List("permissions", "grants", "expired")
//
//      fields.map{ obj \? _ }.collect { 
//        case Some(v) => true
//      }
//
//      if(fields.size == obj.children.size) {
//        ((obj \ "permissions").validated[Option[Permissions]] |@| 
//         (obj \ "grants").validated[Option[Set[UID]]] |@| 
//         (obj \ "expired").validated[Option[Boolean]]).apply(TokenCreate(_,_,_))
//      } else {
//        Failure(Invalid("Unexpected fields in token create object."))
//      }
//    }
//  }        
//}
//
//object TokenCreate extends TokenCreateSerialization with ((Option[Permissions], Option[Set[UID]], Option[Boolean]) => TokenCreate)
//
//case class TokenUpdate(addGrants: Set[UID], removeGrants: Set[UID], expired: Option[Boolean])
//
//trait TokenUpdateSerialization {
//  implicit val TokenUpdateDecomposer: Decomposer[TokenUpdate] = new Decomposer[TokenUpdate] {
//    override def decompose(token: TokenUpdate): JValue = JObject(List(  
//      JField("addGrants", token.addGrants.serialize),
//      JField("removeGrants", token.removeGrants.serialize),
//      JField("expired", token.expired.serialize)
//    ))        
//  }     
//
//  implicit val TokenUpdateExtractor: Extractor[TokenUpdate] = new Extractor[TokenUpdate] with ValidatedExtraction[TokenUpdate] {            
//    override def validated(obj: JValue): Validation[Error, TokenUpdate] =
//      ((obj \ "addGrants").validated[Set[UID]] |@| 
//       (obj \ "removeGrants").validated[Set[UID]] |@|  
//       (obj \ "expired").validated[Option[Boolean]]).apply(TokenUpdate(_,_,_))
//
//  }        
//}
//
//object TokenUpdate extends TokenUpdateSerialization with ((Set[UID], Set[UID], Option[Boolean]) => TokenUpdate)
