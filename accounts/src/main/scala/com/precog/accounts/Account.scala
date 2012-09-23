package com.precog
package accounts

import com.precog.common.Path
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
//import blueeyes.json.xschema.DefaultSerialization.{DateTimeDecomposer => _, DateTimeExtractor => _, _}
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz.Validation
import scalaz.syntax.apply._


case class AccountPlan(planType: String) 
object AccountPlan {
  val Free = AccountPlan("Free")
}


trait AccountPlanSerialization {
  implicit val AccountPlanDecomposer: Decomposer[AccountPlan] = new Decomposer[AccountPlan] {
    override def decompose(t: AccountPlan): JValue = JObject(List(JField("type", t.planType))) 
  }

  implicit val AccountPlanExtractor: Extractor[AccountPlan] = new Extractor[AccountPlan] with ValidatedExtraction[AccountPlan] {    
    override def validated(obj: JValue): Validation[Error, AccountPlan] = 
      ((obj \ "type").validated[String]).map(AccountPlan(_))
  }
}


case class Account(accountId: String, 
                   email: String, 
                   passwordHash: String, 
                   passwordSalt: String, 
                   accountCreationDate: DateTime, 
                   apiKey: String, 
                   rootPath: Path, 
                   plan: AccountPlan) 


trait AccountSerialization extends AccountPlanSerialization {
  implicit val AccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
    override def decompose(t: Account): JValue = JObject(List(
      JField("accountId", t.accountId),
      JField("email", t.email),
      JField("passwordHash", t.passwordHash),
      JField("passwordSalt", t.passwordSalt),
      JField("accountCreationDate", t.accountCreationDate.serialize),
      JField("apiKey", t.apiKey),
      JField("rootPath", t.rootPath),
      JField("plan", t.plan.serialize)
      )) 
  }

  implicit val AccountExtractor: Extractor[Account] = new Extractor[Account] with ValidatedExtraction[Account] {    
    override def validated(obj: JValue): Validation[Error, Account] = 
      ((obj \ "accountId").validated[String] |@|
       (obj \ "email").validated[String] |@|
       (obj \ "passwordHash").validated[String] |@|
       (obj \ "passwordSalt").validated[String] |@|
       (obj \ "accountCreationDate").validated[DateTime] |@|
       (obj \ "apiKey").validated[String] |@|
       (obj \ "rootPath").validated[Path] |@|
       (obj \ "plan").validated[AccountPlan]) {
         Account.apply _
       }
  }
}

object Account extends AccountSerialization


