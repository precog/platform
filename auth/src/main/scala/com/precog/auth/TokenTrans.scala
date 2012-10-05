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

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.xschema.DefaultSerialization._

import java.io._
import scala.io.Source

import com.precog.common._
import com.precog.common.security._

object TokenTrans {
  def main(args: Array[String]) {
    val input = args(0)

    val source = Source.fromFile(input)
    val tokenOutput = new FileWriter(input + ".newtokens")
    val grantOutput = new FileWriter(input + ".newgrants")
    try {
      val oldTokens = source.getLines.map(JsonParser.parse(_)).toList

      val output = process(oldTokens)

      val tokens = output.keySet
      val grants = output.values.flatten

      for (token <- tokens) {
        tokenOutput.write(compact(render(token.serialize(Token.TokenDecomposer))) + "\n")
      }
      for (grant <- grants) {
        grantOutput.write(compact(render(grant.serialize(Grant.GrantDecomposer))) + "\n")
      }
    } finally {
      source.close()
      tokenOutput.close()
      grantOutput.close()
    }
  }

  private def newUUID() = java.util.UUID.randomUUID.toString
  private def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")

  def process(tokens: List[JValue]): Map[Token, Set[Grant]] = {
    tokens.foldLeft(Map.empty[Token, Set[Grant]]) { 
      case (acc, proto) => 
        val JString(uid) = (proto \ "uid") 
        val JString(path) = proto(JPath("permissions.path[0].pathSpec.subtree"))

        val writePermission = proto.find {
          case JString("PATH_WRITE") => true
          case _ => false
        }
        
        val writeGrants = Set( 
          //Grant(newGrantID, Some("write_parent"), WritePermission(Path(path), None)),
          //Grant(newGrantID, Some("owner_parent"), OwnerPermission(Path(path), None))
          Grant(newGrantID, None, WritePermission(Path(path), None)),
          Grant(newGrantID, None, OwnerPermission(Path(path), None))
        )

        val readGrants = Set(
          //Grant(newGrantID, Some("read_parent"), ReadPermission(Path(path), uid, None))
          Grant(newGrantID, None, ReadPermission(Path(path), uid, None))
        )

        val grants = if (writePermission == JNothing) readGrants else readGrants ++ writeGrants

        val token = Token(path.replaceAll("/", " "), uid, "", grants.map{ _.gid }.toSet)        

        acc + (token -> grants)
    }
  }
}
