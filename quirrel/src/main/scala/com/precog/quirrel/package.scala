package com.precog

package object quirrel {
  type TicId = String

  case class Identifier(namespace: Vector[String], id: String)
}
