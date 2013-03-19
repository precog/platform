package com.precog.gjallerhorn

import blueeyes.json._

sealed trait ApiResult {
  def jvalue: JValue = this match {
    case ApiFailure(code, msg) => sys.error("failure %d: %s" format (code, msg))
    case ApiBadJson(e) => sys.error("parse error: %s" format e.getMessage)
    case ApiResponse(j) => j
  }
}
case class ApiFailure(code: Int, msg: String) extends ApiResult
case class ApiBadJson(e: Throwable) extends ApiResult
case class ApiResponse(j: JValue) extends ApiResult
