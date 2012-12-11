package com.precog
package accounts

import com.precog.common.Path
import blueeyes.json._

import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
//import blueeyes.json.serialization.DefaultSerialization.{DateTimeDecomposer => _, DateTimeExtractor => _, _}
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz.Validation
import scalaz.syntax.apply._


case class AccountPlan(planType: String) 
object AccountPlan {
  val Root = AccountPlan("Root")
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
                   plan: AccountPlan, 
                   parentId: Option[String] = None,
                   lastPasswordChangeTime: Option[DateTime] = None)

trait AccountSerialization extends AccountPlanSerialization {
  val UnsafeAccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
    override def decompose(t: Account): JValue = JObject(List(
      Some(JField("accountId", t.accountId)),
      Some(JField("email", t.email)),
      Some(JField("passwordHash", t.passwordHash)),
      Some(JField("passwordSalt", t.passwordSalt)),
      Some(JField("accountCreationDate", t.accountCreationDate.serialize)),
      Some(JField("apiKey", t.apiKey)),
      Some(JField("rootPath", t.rootPath)),
      Some(JField("plan", t.plan.serialize)),
      t.parentId.map(i => JField("parentId", i)),
      t.lastPasswordChangeTime.map(i => JField("lastPasswordChangeTime", i.serialize))).flatten
    ) 
  }

  implicit val AccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
    override def decompose(t: Account): JValue = JObject(List(
      Some(JField("accountId", t.accountId)),
      Some(JField("email", t.email)),
      Some(JField("accountCreationDate", t.accountCreationDate.serialize)),
      Some(JField("apiKey", t.apiKey)),
      Some(JField("rootPath", t.rootPath)),
      Some(JField("plan", t.plan.serialize)),
      t.lastPasswordChangeTime.map(i => JField("lastPasswordChangeTime", i.serialize))
      ).flatten) 
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
       (obj \ "plan").validated[AccountPlan] |@| 
       (obj \ "parentId").validated[Option[String]] |@|
       (obj \ "lastPasswordChangeTime").validated[Option[DateTime]]) {
         Account.apply _
       }
  }
}

object Account extends AccountSerialization


