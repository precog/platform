package com.precog.auth

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import java.io._
import scala.io.Source

import com.precog.common._
import com.precog.common.security._

object APIKeyTrans {
  def main(args: Array[String]) {
    val input = args(0)

    val source = Source.fromFile(input)
    val apiKeyOutput = new FileWriter(input + ".newapikeys")
    val grantOutput = new FileWriter(input + ".newgrants")
    try {
      val oldAPIKeys = source.getLines.map(JParser.parse(_)).toList

      val output = process(oldAPIKeys)

      val apiKeys = output.keySet
      val grants = output.values.flatten

      for (apiKey <- apiKeys) {
        apiKeyOutput.write(apiKey.serialize(APIKeyRecord.apiKeyRecordDecomposer).renderCompact + "\n")
      }
      for (grant <- grants) {
        grantOutput.write(grant.serialize(Grant.GrantDecomposer).renderCompact + "\n")
      }
    } finally {
      source.close()
      apiKeyOutput.close()
      grantOutput.close()
    }
  }

  private def newUUID() = java.util.UUID.randomUUID.toString
  private def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")

  def process(apiKeys: List[JValue]): Map[APIKeyRecord, Set[Grant]] = {
    apiKeys.foldLeft(Map.empty[APIKeyRecord, Set[Grant]]) { 
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

        val grants = if (writePermission == JUndefined) readGrants else readGrants ++ writeGrants

        val apiKey = APIKeyRecord(uid, path.replaceAll("/", " "), "", grants.map{ _.gid }.toSet)        

        acc + (apiKey -> grants)
    }
  }
}
