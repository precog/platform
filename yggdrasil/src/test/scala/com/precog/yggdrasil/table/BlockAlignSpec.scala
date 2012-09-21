/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package table

import com.precog.common._
import com.precog.util._
import com.precog.yggdrasil.util._

import blueeyes.json._
import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.util.Random
import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import SampleData._

trait BlockAlignSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck { self =>
  def testAlign(sample: SampleData) = {
    val module = BlockStoreTestModule.empty[M]

    import module._
    import module.trans._
    import module.trans.constants._

    val lstream = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 => v }
    val rstream = sample.data.zipWithIndex collect { case (v, i) if i % 3 == 0 => v }

    val expected = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 && i % 3 == 0 => v }

    val finalResults = for {
      results <- Table.align(fromJson(lstream), SourceKey.Single, fromJson(rstream), SourceKey.Single)
      leftResult  <- results._1.toJson
      rightResult <- results._2.toJson
      leftResult2 <- results._1.toJson
    } yield {
      (leftResult, rightResult, leftResult2)
    }

    val (leftResult, rightResult, leftResult2) = finalResults.copoint

    leftResult must_== expected
    rightResult must_== expected
    leftResult must_== leftResult2
  }

  def checkAlign = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => testAlign(sample.sortBy(_ \ "key")) }
  }

  def alignSimple = {
    val JArray(elements) = JsonParser.parse("""[
        {
          "value":{ "fr8y":-2.761198250953116839E+14037, "hw":[], "q":2.429467767811669098E+50018 },
          "key":[1.0,2.0]
        },
        {
          "value":{ "fr8y":8862932465119160.0, "hw":[], "q":-7.06989214308545856E+34226 },
          "key":[2.0,1.0]
        },
        {
          "value":{ "fr8y":3.754645750547307163E-38452, "hw":[], "q":-2.097582685805979759E+29344 },
          "key":[2.0,4.0]
        },
        {
          "value":{ "fr8y":0.0, "hw":[], "q":2.839669248714535100E+14955 },
          "key":[3.0,4.0]
        },
        {
          "value":{ "fr8y":-1E+8908, "hw":[], "q":6.56825624988914593E-49983 },
          "key":[4.0,2.0]
        },
        {
          "value":{ "fr8y":123473018907070400.0, "hw":[], "q":0E+35485 },
          "key":[4.0,4.0]
        }]
    """)

    val sample = SampleData(elements.toStream, Some((2,List((JPath(".q"),CNum), (JPath(".hw"),CEmptyArray), (JPath(".fr8y"),CNum)))))

    testAlign(sample.sortBy(_ \ "key"))
  }

  def alignAcrossBoundaries = {
    val JArray(elements) = JsonParser.parse("""[
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.0
        },
        "key":[4.0,26.0,21.0]
      },
      {
        "value":8.715632723857159E+307,
        "key":[5.0,39.0,59.0]
      },
      {
        "value":-1.104890528035041E+307,
        "key":[6.0,50.0,5.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.9570651303391037E+307
        },
        "key":[6.0,59.0,9.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.645581770904817E+307
        },
        "key":[9.0,28.0,29.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.0
        },
        "key":[10.0,21.0,30.0]
      },
      {
        "value":7.556003912577644E+307,
        "key":[11.0,51.0,15.0]
      },
      {
        "value":3.4877741123656093E+307,
        "key":[11.0,52.0,28.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":8.263625900450069E+307
        },
        "key":[12.0,25.0,58.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[16.0,31.0,42.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-7.976677386824275E+307
        },
        "key":[21.0,7.0,10.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":5.359853255240687E+307
        },
        "key":[23.0,12.0,26.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":8.988465674311579E+307
        },
        "key":[25.0,39.0,48.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[26.0,13.0,32.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":6.740770480418812E+307
        },
        "key":[27.0,17.0,7.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[27.0,36.0,16.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[32.0,13.0,11.0]
      },
      {
        "value":-1.0,
        "key":[32.0,50.0,21.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.0
        },
        "key":[37.0,5.0,43.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":2.911458319070367E+307
        },
        "key":[41.0,17.0,32.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":6.878966108624357E+307
        },
        "key":[48.0,14.0,45.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.024686561894592E+307
        },
        "key":[51.0,26.0,46.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.8366746041516641E+307
        },
        "key":[52.0,50.0,54.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.0
        },
        "key":[53.0,54.0,27.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.87789254395668E+307
        },
        "key":[55.0,46.0,57.0]
      }]
    """)

    val sample = SampleData(elements.toStream, Some((3,List((JPath(".xb5hs2ckjajs0k44x"),CDouble), (JPath(".zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump"),CEmptyArray), (JPath(".sp7hpv"),CEmptyObject)))))
    testAlign(sample.sortBy(_ \ "key"))
  }
}

object BlockAlignSpec extends TableModuleSpec[Free.Trampoline] with BlockAlignSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  "align" should {
    "a simple example" in alignSimple
    "across slice boundaries" in alignAcrossBoundaries
    "survive a trivial scalacheck" in checkAlign
  }
}
// vim: set ts=4 sw=4 et:
