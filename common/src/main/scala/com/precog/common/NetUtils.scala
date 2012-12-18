package com.precog.common

import blueeyes.core.http.HttpRequest

object NetUtils {
  def remoteIpFrom(req: HttpRequest[_]) = req.remoteHost.map(_.getHostAddress).getOrElse("Unknown")
}
