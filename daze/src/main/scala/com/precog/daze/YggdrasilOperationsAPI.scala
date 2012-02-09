package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Duration
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import Iteratee._

trait YggEnumOpsConfig {
  def flatMapTimeout: Duration
  def sortBufferSize: Int
  def scratchDir: File
}

trait YggdrasilEnumOpsComponent extends YggConfigComponent with DatasetEnumOpsComponent {
  type YggConfig <: YggEnumOpsConfig

  trait Ops extends DatasetEnumOps {
    def flatMap[X, E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] = 
      DatasetEnum(d.fenum.map(_.flatMap(e => Await.result(f(e).fenum, yggConfig.flatMapTimeout))))

    def sort[X](d: DatasetEnum[X, SEvent, IO], memoId: Option[Int])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] = 
      DatasetEnum(d.fenum.map(Enumerators.sort[X](_, yggConfig.sortBufferSize, yggConfig.scratchDir, d.descriptor)))
    
    def memoize[X](d: DatasetEnum[X, SEvent, IO], memoId: Int): DatasetEnum[X, SEvent, IO] = d      // TODO
  }
}

// vim: set ts=4 sw=4 et:
