package com.precog.common.security

import com.precog.analytics.Path

trait TokenManagerComponent {
  def tokenManager: TokenManager
}

trait TokenManager {
  def lookup(uid: UID): Option[Token]
}

object StaticTokenManager extends TokenManager {

  val rootUID = "C18ED787-BF07-4097-B819-0415C759C8D5"
  
  val testUID = "03C4F5FE-69E2-4151-9D93-7C986936CB86"
  val usageUID = "6EF2E81E-D9E8-4DC6-AD66-DEB30A164F73"
  
  val publicUID = "B88E82F0-B78B-49D9-A36B-78E0E61C4EDC"
 
  val cust1UID = "C5EF0038-A2A2-47EB-88A4-AAFCE59EC22B"
  val cust2UID = "1B10E413-FB5B-4769-A887-8AFB587CF00A"
  
  def standardAccountPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare), 
      MayAccessPath(Subtree(Path(path)), PathWrite, mayShare)
    )(
      MayAccessData(Subtree(Path("/")), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  def publishPathPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare)
    )(
      MayAccessData(Subtree(Path(path)), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  private val config = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
    (rootUID, None, standardAccountPerms("/", rootUID, true), Set(), false),
    (publicUID, Some(rootUID), publishPathPerms("/public", rootUID, true), Set(), false),
    (testUID, Some(rootUID), standardAccountPerms("/unittest", testUID, true), Set(), false),
    (usageUID, Some(rootUID), standardAccountPerms("/__usage_tracking__", usageUID, true), Set(), false),
    (cust1UID, Some(rootUID), standardAccountPerms("/user1", cust1UID, true), Set(publicUID), false),
    (cust2UID, Some(rootUID), standardAccountPerms("/user2", cust2UID, true), Set(publicUID), false)
  )

  private lazy val map = Map( config map {
    case (uid, issuer, perms, grants, canShare) => (uid -> Token(uid, issuer, perms, grants, canShare))
  }: _*)
  
  def lookup(uid: UID) = map.get(uid)

}
