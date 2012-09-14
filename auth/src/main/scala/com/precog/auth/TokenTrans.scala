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

        val token = Token(uid, path.replaceAll("/", " "), grants.map{ _.gid }.toSet)        

        acc + (token -> grants)
    }
  }
}
