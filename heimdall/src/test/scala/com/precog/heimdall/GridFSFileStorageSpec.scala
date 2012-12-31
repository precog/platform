package com.precog.heimdall

import com.precog.common._
import com.precog.common.jobs._

import org.specs2.mutable._

import blueeyes.persistence.mongo._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._

class GridFSFileStorageSpec extends Specification with RealMongoSpecSupport {
  include(new FileStorageSpec[Need] {
    val M: Monad[Need] with Copointed[Need] = Need.need
    lazy val fs = GridFSFileStorage(realMongo.getDB("gridFsFileStorageSpec"))(M)
  })
}
