package com.precog.auth

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.xschema.DefaultSerialization._

import java.io.FileReader

import com.precog.common._
import com.precog.common.security._

object TokenTrans {
  def main(args: Array[String]) {
    val input = args(0)

    val reader = new FileReader(input)
    try {
      val JArray(oldTokens) = JsonParser.parse(reader)

      val output = process(oldTokens)

      val tokens = output.keySet
      val grants = output.values.flatten

      for (token <- tokens) {
        println(compact(render(token.serialize(Token.UnsafeTokenDecomposer))))
      }
      println("############")
      for (grant <- grants) {
        println(compact(render(grant.serialize(Grant.UnsafeGrantDecomposer))))
      }
    } finally {
      reader.close()
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
          Grant(newGrantID, Some("write_parent"), WritePermission(Path(path), None)),
          Grant(newGrantID, Some("owner_parent"), OwnerPermission(Path(path), None))
        )

        val readGrants = Set(
          Grant(newGrantID, Some("read_parent"), ReadPermission(Path(path), uid, None))
        )

        val grants = if (writePermission == JNothing) readGrants else readGrants ++ writeGrants

        val token = Token(uid, path.replaceAll("/", " "), grants.map{ _.gid }.toSet)        

        acc + (token -> grants)
    }
  }
}
