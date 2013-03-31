package com.precog.gjallerhorn

case class Account(user: String, password: String, accountId: String, apiKey: String, rootPath: String) {
  def bareRootPath = rootPath.substring(1, rootPath.length - 1)
}
