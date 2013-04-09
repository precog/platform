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
package com.precog.auth

import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.services.ServiceHandlerUtil._

import blueeyes.bkka._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._
import blueeyes.util.Clock

import akka.dispatch.{ ExecutionContext, Future, Promise }

import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.Validation._
import scalaz.std.boolean._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.apply._
import scalaz.syntax.id._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import shapeless._

class SecurityServiceHandlers(val apiKeyManager: APIKeyManager[Future], val clock: Clock)(implicit executor: ExecutionContext) {
  import com.precog.common.security.service.v1
  type R = HttpResponse[JValue]

  private implicit val M0: Monad[Future] = new FutureMonad(executor)
  private val apiKeyFinder = new DirectAPIKeyFinder(apiKeyManager)

  import apiKeyFinder.{findAPIKey, findAllAPIKeys, grantDetails, recordDetails}

  object ReadAPIKeysHandler extends CustomHttpService[Future[JValue], APIKey => Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success { (authAPIKey: APIKey) =>
      findAllAPIKeys(authAPIKey) map { keySet =>
        ok(keySet.nonEmpty.option(keySet))
      }
    }

    val metadata = DescriptionMetadata("Retrieves the children of the authorizing API key.")
  }

  trait CreateHandler extends CustomHttpService[Future[JValue], APIKey => Future[R]] with Logging {
    protected def create(authAPIKey: APIKey, requestBody: JValue): Future[R]

    protected def missingContentMessage: String

    def service = (request: HttpRequest[Future[JValue]]) => Success { (authAPIKey: APIKey) =>
      for {
        content  <- request.content.toSuccess(badRequest(missingContentMessage)).sequence[Future, JValue]
        response <- content.map(create(authAPIKey, _)).sequence[Future, R]
      } yield {
        response.toEither.merge
      }
    }
  }

  object CreateAPIKeyHandler extends CreateHandler {
    protected def create(authAPIKey: APIKey, requestBody: JValue): Future[R] = {
      requestBody.validated[v1.NewAPIKeyRequest] match {
        case Success(request) =>
          if (request.grants.exists(_.isExpired(some(clock.now())))) {
            Promise successful badRequest("Error creating new API key.", Some("Unable to create API key with expired permission"))
          } else {
            apiKeyManager.newAPIKeyWithGrants(request.name, request.description, authAPIKey, request.grants.toSet) flatMap { k =>
              if (k.isDefined) {
                (k collect recordDetails sequence) map { ok[v1.APIKeyDetails] }
              } else {
                Promise successful badRequest("Error creating new API key.", Some("Requestor lacks permission to assign given grants to API key"))
              }
            }
          }

        case Failure(e) =>
          logger.warn("The API key request body \n" + requestBody.renderPretty + "\n was invalid: " + e)
          Promise successful badRequest("Invalid new API key request body.", Some(e.message))
      }
    }

    protected val missingContentMessage = "Missing new API key request body."

    val metadata = DescriptionMetadata("Creates a new API key as a child of the specified authorizing API key.")
  }

  object ReadAPIKeyDetailsHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success {
      // since having an api key means you can see the details, we don't check perms.
      request.parameters.get('apikey).map { apiKey =>
        // The authkey is intentionally undocumented and only used internally.
        apiKeyFinder.findAPIKey(apiKey, request.parameters.get('authkey)).map { k =>
          if (k.isDefined) ok(k) else notFound("Unable to find API key "+apiKey)
        }
      } getOrElse {
        Promise successful badRequest("Missing API key from request URL.")
      }
    }

    val metadata = DescriptionMetadata("Returns the details of the API key specified in the path.")
  }

  object DeleteAPIKeyHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success {
      request.parameters.get('apikey) map { apiKey =>
        apiKeyManager.deleteAPIKey(apiKey) map { k =>
          if (k.isDefined) noContent else notFound("Unable to find API key "+apiKey)
        }
      } getOrElse {
        Promise successful badRequest("Missing API key from request URL.")
      }
    }

    val metadata = DescriptionMetadata("Deletes the specified API key.")
  }

  object ReadAPIKeyGrantsHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success {
      request.parameters.get('apikey) map { apiKey =>
        findAPIKey(apiKey, None) map {
          case Some(v1.APIKeyDetails(_, _, _, grantDetails, _)) => ok(Some(grantDetails))
          case None => notFound("The specified API key does not exist")
        }
      } getOrElse {
        Promise successful badRequest("Missing API key from request URL.")
      }
    }

    val metadata = DescriptionMetadata("Returns the set of grants associated with the API key resource specified by the URL path.")
  }

  object CreateAPIKeyGrantHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    private def create(apiKey: APIKey, requestBody: JValue): Future[R] = {
      requestBody.validated[GrantId]("grantId") match {
        case Success(grantId) =>
          apiKeyManager.addGrants(apiKey, Set(grantId)) map { g =>
            if (g.isDefined) created[JValue](None) else badRequest("unable to add grant " + grantId + " to API key " + apiKey)
          }

        case Failure(e) =>
          logger.warn("Unable to parse grant ID from \n" + requestBody.renderPretty + "\n: " + e)
          Promise successful badRequest("Invalid add grant request body.", Some("Invalid add grant request body: " + e))
      }
    }

    val service = (request: HttpRequest[Future[JValue]]) => Success {
      val apiKeyV = request.parameters.get('apikey).toSuccess(badRequest("Missing API key from request URL"))
      for {
        contentV <- request.content.toSuccess(badRequest("Missing body content for grant creation.")).sequence[Future, JValue]
        response <- (for (apiKey <- apiKeyV; content <- contentV) yield create(apiKey, content)).sequence[Future, R]
      } yield response.toEither.merge
    }

    val metadata = DescriptionMetadata("Adds a new grant to the API key resource specified by the URL path.")
  }

  object DeleteAPIKeyGrantHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success {
      Apply[Option].apply2(request.parameters.get('apikey), request.parameters.get('grantId)) { (apiKey, grantId) =>
        apiKeyManager.removeGrants(apiKey, Set(grantId)) map { k =>
          if (k.isDefined) noContent
          else badRequest("Invalid remove grant request.", Some("Unable to remove grant "+grantId+" from API key "+apiKey))
        }
      } getOrElse {
        Promise successful badRequest("Missing API key or grant ID from request URL")
      }
    }

    val metadata = DescriptionMetadata("Deletes the grant resource specified by the URL path from the API key resource specified by the URL path.")
  }

  object ReadGrantsHandler extends CustomHttpService[Future[JValue], APIKey => Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success { (authAPIKey: APIKey) =>
      findAllAPIKeys(authAPIKey) map { allKeys =>
        ok(Some(allKeys.flatMap(_.grants)))
      }
    }

    val metadata = DescriptionMetadata("Returns the list of grants available to the authorizing API key.")
  }

  object CreateGrantHandler extends CreateHandler {
    protected def create(authAPIKey: APIKey, requestBody: JValue): Future[R] = {
      requestBody.validated[v1.NewGrantRequest] match {
        case Success(request) =>
          apiKeyManager.deriveGrant(request.name, request.description, authAPIKey, request.permissions, request.expirationDate) map { g =>
            if (g.isDefined) ok(g map grantDetails)
            else badRequest("Error creating new grant.", Some("Requestor lacks permissions to create grant"))
          }

        case Failure(e) =>
          logger.warn("The grant creation request body \n" + requestBody.renderPretty + "\n was invalid: " + e)
          Promise successful badRequest("Invalid new grant request body.", Some(e.message))
      }
    }

    protected val missingContentMessage = "Missing grant request body."

    val metadata = DescriptionMetadata("Creates a new grant derived from permissions conferred by the grants of the authorizing API key.")
  }

  object ReadGrantDetailsHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success {
      request.parameters.get('grantId) map { grantId =>
        apiKeyManager.findGrant(grantId) map { g =>
          if (g.isDefined) ok(g map grantDetails)
          else notFound("Unable to find grant " + grantId)
        }
      } getOrElse {
        Promise successful badRequest("Missing grant ID from request URL.")
      }
    }

    val metadata = DescriptionMetadata("Returns the details of the grant resource specified in the request URL.")
  }

  object ReadGrantChildrenHandler extends CustomHttpService[Future[JValue], Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success {
      request.parameters.get('grantId) map { grantId =>
        apiKeyManager.findGrantChildren(grantId) map {
          grants => ok(Some(grants map grantDetails))
        }
      } getOrElse {
        Promise successful badRequest("Missing grant ID from request URL.")
      }
    }

    val metadata = DescriptionMetadata("Returns the set of child grants derived from the grant resource specified in the request URL.")
  }

  object CreateGrantChildHandler extends CustomHttpService[Future[JValue], APIKey => Future[R]] with Logging {
    def create(issuerKey: APIKey, parentId: GrantId, requestBody: JValue): Future[R] = {
      requestBody.validated[v1.NewGrantRequest] match {
        case Success(r) =>
          apiKeyManager.deriveSingleParentGrant(None, None, issuerKey, parentId, r.permissions, r.expirationDate) map { g =>
            if (g.isDefined) ok(g map grantDetails)
            else badRequest("Error creating new child grant.", Some("Requestor lacks permissions to create grant."))
          }

        case Failure(e) =>
          Promise successful badRequest("Invalid new child grant request body.", Some(e.message))
      }
    }

    val service = (request: HttpRequest[Future[JValue]]) => Success { (authAPIKey: APIKey) =>
      val parentIdV = request.parameters.get('grantId).toSuccess(badRequest("Missing grant ID from request URL"))
      for {
        contentV <- request.content.toSuccess(badRequest("Missing body content for grant creation.")).sequence[Future, JValue]
        response <- (for (parentId <- parentIdV; content <- contentV) yield create(authAPIKey, parentId, content)).sequence[Future, R]
      } yield response.toEither.merge
    }

    val metadata = DescriptionMetadata("Creates a new grant as a child of the grant resource specified in the request URL, and deriving its permissions therefrom.")
  }

  object DeleteGrantHandler extends CustomHttpService[Future[JValue], APIKey => Future[R]] with Logging {
    private def deleteGrant(grantId: GrantId) = apiKeyManager.deleteGrant(grantId) map { s =>
      //TODO: Is the badRequest message here really appropriate?
      if (s.nonEmpty) noContent else badRequest("Unable to find grant " + grantId + " for deletion.")
    }

    val service = (request: HttpRequest[Future[JValue]]) => Success { (authAPIKey: APIKey) =>
      request.parameters.get('grantId) map { grantId =>
        apiKeyManager.findGrant(grantId) flatMap {
          case Some(grant) =>
            if (grant.issuerKey == authAPIKey) deleteGrant(grantId)
            else {
              apiKeyManager.findAPIKeyAncestry(grant.issuerKey) flatMap { ancestry =>
                if (ancestry.exists(_.apiKey == authAPIKey)) deleteGrant(grantId)
                else Promise successful badRequest("Requestor does not have permission to delete grant " + grantId)
              }
            }

          case None =>
            Promise successful badRequest("Unable to find grant " + grantId + " for deletion.")
        }
      } getOrElse {
        Promise successful badRequest("Missing grant ID from request URL.")
      }
    }

    val metadata = DescriptionMetadata("Deletes the grant resource specified in the request URL.")
  }

  object ReadPermissionsHandler extends CustomHttpService[Future[JValue], (APIKey, Path) => Future[R]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => Success { (authAPIKey: APIKey, path: Path) =>
      val atO: Option[Validation[Extractor.Error, DateTime]] = request.parameters.get('at).map(JString(_).validated[DateTime])
      atO.getOrElse(Success(clock.now())) match {
        case Success(at) =>
          apiKeyManager.validGrants(authAPIKey, Some(at)) map { grants =>
            val pathPermissions = grants flatMap (_.permissions) filter { perm =>
              (perm.path == path) || path.isChildOf(perm.path)
            }
            ok(Some(pathPermissions))
          }

        case Failure(e) =>
          logger.warn("The 'at paramter was not a valid DateTime: " + e)
          Promise successful badRequest("Invalid date provided to 'at parameter.", Some(e.message))
      }
    }

    val metadata = AndMetadata(
      AboutMetadata(
        ParameterMetadata('at, Some("Defaults to the current time.")),
        DescriptionMetadata("The timestamp against which permissions should be checked, in ISO8601 datetime format.")
      ),
      DescriptionMetadata("Retrieves the full set of permissions available to the authorizing API key.")
    )
  }
}
