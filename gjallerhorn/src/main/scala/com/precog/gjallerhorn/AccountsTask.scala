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
package com.precog.gjallerhorn

import blueeyes.json._
import dispatch._
import org.specs2.mutable._
import scalaz._
import specs2._

class AccountsTask(settings: Settings) extends Task(settings: Settings) with Specification {
  "accounts web service" should {
    "create accounts" in {
      val (user, pass) = generateUserAndPassword

      val post = (accounts / "") << """{ "email": "%s", "password": "%s" }""".format(user, pass)
      val result = Http(post OK as.String)
      val json = JParser.parseFromString(result())

      json must beLike {
        case Success(jobj) => (jobj \ "accountId") must beLike { case JString(_) => ok }
      }
    }
  }
}

object RunAccounts {
  def main(args: Array[String]) {
    try {
    val settings = Settings.fromFile(new java.io.File("shard.out"))
      run(
        new AccountsTask(settings)
      )
    } finally {
      Http.shutdown()
    }
  }
}
