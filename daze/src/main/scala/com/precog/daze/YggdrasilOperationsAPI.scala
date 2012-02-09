package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._

import akka.dispatch.Await
import akka.dispatch.Future
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import Iteratee._

trait AkkaConfig {
  implicit def asyncContext: akka.dispatch.ExecutionContext
}

trait YggdrasilEnumOps extends DatasetEnumOps {
  def yggConfig: YggConfig

  def sort[X](d: DatasetEnum[X, SEvent, IO], memoId: Option[Int])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] = {
    DatasetEnum(d.fenum.map(Enumerators.sort[X](_, yggConfig.sortBufferSize, yggConfig.scratchDir, d.descriptor)))
  }
  
  def memoize[X](d: DatasetEnum[X, SEvent, IO], memoId: Int): DatasetEnum[X, SEvent, IO] = d      // TODO
}

// vim: set ts=4 sw=4 et:
