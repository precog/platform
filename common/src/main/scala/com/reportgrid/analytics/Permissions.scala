package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import scalaz.Scalaz._
import scalaz.Validation

/** Permissions dictate how a token may be used. Read permission allows
 * a token to read data. Write permission allows a token to write data.
 * Share permission allows a token to create new tokens with the same
 * or weaker level of permission.
 */
case class Permissions(read: Boolean, write: Boolean, share: Boolean, explore: Boolean) {
  /** Issues new permissions derived from this one. The new permissions 
   * cannot be broader than these permissions.
   */
  def issue(read: Boolean, write: Boolean, share: Boolean, explore: Boolean): Permissions = Permissions(
    read  = this.read  && read,
    write = this.write && write,
    share = this.share && share,
    explore = this.explore && explore
  )
  
  /** Limits these permissions to the specified permissions. 
   */
  def limitTo(that: Permissions) = that.issue(read, write, share, explore)
}

trait PermissionsSerialization {
    final implicit val PermissionsDecomposer = new Decomposer[Permissions] {
    def decompose(permissions: Permissions): JValue = JObject(
      JField("read",    permissions.read.serialize)  ::
      JField("write",   permissions.write.serialize) ::
      JField("share",   permissions.share.serialize) ::
      JField("explore",   permissions.explore.serialize) ::
      Nil
    )
  }

  final implicit val PermissionsExtractor = new Extractor[Permissions] {
    def extract(jvalue: JValue): Permissions = Permissions(
      read  = (jvalue \ "read").deserialize[Boolean],
      write = (jvalue \ "write").deserialize[Boolean],
      share = (jvalue \ "share").deserialize[Boolean],
      explore = (jvalue \ "explore").deserialize[Boolean]
    )
  }

  def permissionsExtractor(default: Permissions) = new Extractor[Permissions] {
    override def extract(jvalue: JValue): Permissions = Permissions(
      read  = (jvalue \ "read").validated[Boolean]      | default.read,
      write = (jvalue \ "write").validated[Boolean]     | default.write,
      share = (jvalue \ "share").validated[Boolean]     | default.share,
      explore = (jvalue \ "explore").validated[Boolean] | default.explore
    )
  }
}

object Permissions extends PermissionsSerialization {
  val All = Permissions(true, true, true, true)
}

