package com.precog.common
package accounts

import com.precog.common.Path
import com.precog.common.security.{ APIKey, Permission, ReadPermission, WritePermission, DeletePermission }
import Permission._

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Versioned._

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz.Validation
import scalaz.syntax.apply._
import scalaz.syntax.plus._

import shapeless._

case class AccountPlan(planType: String)
object AccountPlan {
  val Root = AccountPlan("Root")
  val Free = AccountPlan("Free")

  implicit val iso = Iso.hlist(AccountPlan.apply _, AccountPlan.unapply _)
  val schema = "type" :: HNil
  implicit val (decomposer, extractor) = serializationV[AccountPlan](schema, None)
}

case class Account(accountId: AccountId,
                   email: String,
                   passwordHash: String,
                   passwordSalt: String,
                   accountCreationDate: DateTime,
                   apiKey: APIKey,
                   rootPath: Path,
                   plan: AccountPlan,
                   parentId: Option[String] = None,
                   lastPasswordChangeTime: Option[DateTime] = None,
                   profile: Option[JValue] = None)

object Account {
  implicit val iso = Iso.hlist(Account.apply _, Account.unapply _)
  val schemaV1     = "accountId" :: "email" :: "passwordHash" :: "passwordSalt" :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" :: "parentId" :: "lastPasswordChangeTime" :: "profile" :: HNil

  val extractorPreV = extractorV[Account](schemaV1, None)
  val extractorV1 = extractorV[Account](schemaV1, Some("1.1".v))
  implicit val accountExtractor = extractorV1 <+> extractorPreV

  implicit val decomposerV1 = decomposerV[Account](schemaV1, Some("1.1".v))

  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  // FIXME: Remove when there are no SHA1 hashes in the accounts db
  def saltAndHashSHA1(password: String, salt: String): String = {
    Hashing.sha1().hashString(password + salt, Charsets.UTF_8).toString
  }

  def saltAndHashSHA256(password: String, salt: String): String = {
    Hashing.sha256().hashString(password + salt, Charsets.UTF_8).toString
  }

  // FIXME: Remove when there are no old-style SHA256 hashes in the accounts db
  def saltAndHashLegacy(password: String, salt: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256");
    val dataBytes = (password + salt).getBytes("UTF-8")
    md.update(dataBytes, 0, dataBytes.length)
    val hashBytes = md.digest()

    hashBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  def newAccountPermissions(accountId: AccountId, accountPath: Path): Set[Permission] = {
    // Path is "/" so that an account may read data it wrote no matter what path it exists under. 
    // See AccessControlSpec, NewGrantRequest
    Set[Permission](
      WritePermission(accountPath, WriteAsAny),
      DeletePermission(accountPath, WrittenByAny),
      ReadPermission(Path.Root, WrittenByAccount(accountId))
    )
  }
}

case class WrappedAccountId(accountId: AccountId)

object WrappedAccountId {
  implicit val wrappedAccountIdIso = Iso.hlist(WrappedAccountId.apply _, WrappedAccountId.unapply _)

  val schema = "accountId" :: HNil

  implicit val (wrappedAccountIdDecomposer, wrappedAccountIdExtractor) = serializationV[WrappedAccountId](schema, None)
}

