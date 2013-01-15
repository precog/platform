package com.precog.auth

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.security._

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.core.service._
import blueeyes.json.serialization._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.PlusEmpty._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.plus._

/*
object WebAPIKeyManager extends client.BaseClient {
  def apply(config: Configuration)(implicit executor: ExecutionContext): APIKeyManager[Future] = {
  }
}

class WebAPIKeyManager(authAPIKey: APIKey, client: HttpClient[JValue])(implicit val executor: ExecutionContext) extends APIKeyManager[Future] with Logging {
  implicit val M = new FutureMonad(executor)

  def listAPIKeys = client.query("apiKey", authAPIKey).get("/apikeys/")
    
  def createAPIKey(authAPIKey: String, request: NewAPIKeyRequest) =
    createAPIKeyRaw(authAPIKey, request.serialize)

  def getAPIKeyDetails(authAPIKey: String, queryKey: String) = 
    client.query("apiKey", authAPIKey).get("/apikeys/"+queryKey)

  def getAPIKeyGrants(authAPIKey: String, queryKey: String) = 
    client.query("apiKey", authAPIKey).get("/apikeys/"+queryKey+"/grants/")

  def addAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: WrappedGrantId) = 
    addAPIKeyGrantRaw(authAPIKey, updateKey, grantId.serialize)

  def createAPIKeyGrant(authAPIKey: String, request: NewGrantRequest) = 
    createAPIKeyGrantRaw(authAPIKey, request.serialize)

  def removeAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: String) = 
    client.query("apiKey", authAPIKey).delete("/apikeys/"+updateKey+"/grants/"+grantId)

  def getGrantDetails(authAPIKey: String, grantId: String) = 
    client.query("apiKey", authAPIKey).get("/grants/"+grantId)

  def getGrantChildren(authAPIKey: String, grantId: String) = 
    client.query("apiKey", authAPIKey).get("/grants/"+grantId+"/children/")

  def addGrantChild(authAPIKey: String, grantId: String, request: NewGrantRequest) =
    addGrantChildRaw(authAPIKey, grantId, request.serialize)
    
  def deleteGrant(authAPIKey: String, grantId: String) =
    client.query("apiKey", authAPIKey).delete("/grants/"+grantId)

  def equalGrant(g1: Grant, g2: Grant) = (g1.grantId == g2.grantId) && (g1.permissions == g2.permissions) && (g1.expirationDate == g2.expirationDate)
  
  def mkNewGrantRequest(grant: Grant) = grant match {
    case Grant(_, name, description, _, parentIds, permissions, expirationDate) =>
      NewGrantRequest(name, description, parentIds, permissions, expirationDate)
  }


  private def createAPIKeyRaw(authAPIKey: String, request: JValue) = 
    client.query("apiKey", authAPIKey).post("/apikeys/")(request)(identity[JValue], identityHttpTranscoder[JValue])

  private def addAPIKeyGrantRaw(authAPIKey: String, updateKey: String, grantId: JValue) = 
    client.query("apiKey", authAPIKey).
      post("/apikeys/"+updateKey+"/grants/")(grantId)(identity[JValue], identityHttpTranscoder[JValue])

  private def createAPIKeyGrantRaw(authAPIKey: String, request: JValue) = 
    client.query("apiKey", authAPIKey).
      post("/grants/")(request)(identity[JValue], identityHttpTranscoder[JValue])

  private def addGrantChildRaw(authAPIKey: String, grantId: String, request: JValue) = 
    client.query("apiKey", authAPIKey).
      post("/grants/"+grantId+"/children/")(request)(identity[JValue], identityHttpTranscoder[JValue])
    
}
*/

// vim: set ts=4 sw=4 et:
