package com.precog.yggdrasil

import blueeyes.json.JPath

sealed trait CPath
case class JCPath(jpath: JPath) extends CPath
case class DynCPath(id: Long) extends CPath

// vim: set ts=4 sw=4 et:
