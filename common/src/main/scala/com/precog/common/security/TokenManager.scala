package com.precog.common.security

trait TokenManagerComponent {
  def tokenManager: TokenManager
}

trait TokenManager {
  def lookup(uid: UID): Option[Token]
}

