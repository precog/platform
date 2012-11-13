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
                   plan: AccountPlan) {
}


trait AccountSerialization extends AccountPlanSerialization {
  val UnsafeAccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
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

  implicit val AccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
    override def decompose(t: Account): JValue = JObject(List(
      JField("accountId", t.accountId),
      JField("email", t.email),
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


