package com.precog
package auth

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

class GetTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      Future(HttpResponse[JValue](OK, content = Some(t.serialize)))
    }
  }
  val metadata = None
}

class CreateTokenHandler(tokenManager: TokenManager, accessControl: AccessControl)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging with HandlerHelpers {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      request.content.map { _.flatMap { _.validated[TokenCreate] match {
        case Success(tokenCreate) =>
          tokenCreate.permissions.map{ Future(_) }.getOrElse{ tokenManager.sharablePermissions(t) }.flatMap{ perms =>
            accessControl.mayGrant(t.uid, perms) flatMap {
              case false => Future[Resp]((Unauthorized, "The specified token may not grant the requested permissions"))
              case true  =>
                tokenManager.issueNew(Some(t.uid), perms, 
                                      tokenCreate.grants.getOrElse(Set()), 
                                      tokenCreate.expired.getOrElse(false)) map { 
                  case Success(t) => (OK, t.serialize): Resp
                  case Failure(e) => (BadRequest, e): Resp
                }
            }
          }
        case Failure(e)           => Future[Resp](BadRequest, e)
      }}}.getOrElse(Future[Resp]((BadRequest -> "Missing token create request body.")))
    }
  }

  val metadata = None
}

class DeleteTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      request.parameters.get('delete) match {
        case None         =>
          Future(HttpResponse[JValue](BadRequest, content=Some(JString("Token to be deleted was not found."))))
        case Some(delete) =>
          tokenManager.deleteDescendant(t, delete) map { deleted =>
            val content = JArray(deleted.map{ t => JString(t.uid) })
            HttpResponse[JValue](OK, content=Some(content)) 
          }
      }     
    }
  }
  val metadata = None
}

class UpdateTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging with HandlerHelpers {


  val service = (request: HttpRequest[Future[JValue]]) => {
    request.parameters.get('update) match {
      case None            =>
        Failure(inapplicable)
      case Some(updateUID) => 
        Success {(t: Token) => tokenManager.lookup(updateUID) flatMap {
          case None              => Future[Resp]((BadRequest, "Token to update doesn't exists."))
          case Some(updateToken) => request.content.map { _.flatMap { _.validated[TokenUpdate] match {
            case Success(tokenUpdate) =>
              val toUpdate = applyUpdates(updateToken, tokenUpdate)
              tokenManager.updateToken(t, toUpdate) map {
                case Success(rt) => (OK, rt.serialize): Resp
                case Failure(e) => (BadRequest, e): Resp
              }
            case Failure(e)  => Future[Resp]((BadRequest, e))
          }}}.getOrElse(Future[Resp]((BadRequest -> "Missing token update request body.")))
        }
      }
    }
  }

  val metadata = None

  def applyUpdates(t: Token, tu: TokenUpdate): Token = {
    Token(t.uid, t.issuer, t.permissions, (t.grants ++ tu.addGrants) -- tu.removeGrants, tu.expired.getOrElse(t.expired))
  }
}

trait HandlerHelpers {
  
  type Resp = HttpResponse[JValue]
  
  implicit def codeAndMessageToResponse(t: (HttpStatusCode, String)): HttpResponse[JValue] = HttpResponse(t._1, content = Some(JString(t._2)))
  implicit def codeAndValueToResponse(t: (HttpStatusCode, JValue)): HttpResponse[JValue] = HttpResponse(t._1, content = Some(t._2))
  implicit def codeAndErrorToResponse(t: (HttpStatusCode, Error)): HttpResponse[JValue] = HttpResponse(t._1, content = Some(t._2.message))

}

case class TokenCreate(permissions: Option[Permissions], grants: Option[Set[UID]], expired: Option[Boolean])

trait TokenCreateSerialization {
  implicit val TokenCreateDecomposer: Decomposer[TokenCreate] = new Decomposer[TokenCreate] {
    override def decompose(token: TokenCreate): JValue = JObject(List(  
      JField("permissions", token.permissions.serialize),
      JField("grants", token.grants.serialize),
      JField("expired", token.expired.serialize)
    ))        
  }     

  implicit val TokenCreateExtractor: Extractor[TokenCreate] = new Extractor[TokenCreate] with ValidatedExtraction[TokenCreate] {            
    override def validated(obj: JValue): Validation[Error, TokenCreate] = {
      val fields = List("permissions", "grants", "expired")

      fields.map{ obj \? _ }.collect { 
        case Some(v) => true
      }

      if(fields.size == obj.children.size) {
        ((obj \ "permissions").validated[Option[Permissions]] |@| 
         (obj \ "grants").validated[Option[Set[UID]]] |@| 
         (obj \ "expired").validated[Option[Boolean]]).apply(TokenCreate(_,_,_))
      } else {
        Failure(Invalid("Unexpected fields in token create object."))
      }
    }
  }        
}

object TokenCreate extends TokenCreateSerialization with ((Option[Permissions], Option[Set[UID]], Option[Boolean]) => TokenCreate)

case class TokenUpdate(addGrants: Set[UID], removeGrants: Set[UID], expired: Option[Boolean])

trait TokenUpdateSerialization {
  implicit val TokenUpdateDecomposer: Decomposer[TokenUpdate] = new Decomposer[TokenUpdate] {
    override def decompose(token: TokenUpdate): JValue = JObject(List(  
      JField("addGrants", token.addGrants.serialize),
      JField("removeGrants", token.removeGrants.serialize),
      JField("expired", token.expired.serialize)
    ))        
  }     

  implicit val TokenUpdateExtractor: Extractor[TokenUpdate] = new Extractor[TokenUpdate] with ValidatedExtraction[TokenUpdate] {            
    override def validated(obj: JValue): Validation[Error, TokenUpdate] =
      ((obj \ "addGrants").validated[Set[UID]] |@| 
       (obj \ "removeGrants").validated[Set[UID]] |@|  
       (obj \ "expired").validated[Option[Boolean]]).apply(TokenUpdate(_,_,_))

  }        
}

object TokenUpdate extends TokenUpdateSerialization with ((Set[UID], Set[UID], Option[Boolean]) => TokenUpdate)
