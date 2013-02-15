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
package com.precog.niflheim

import java.io._
import blueeyes.json._
import com.precog.common._
import com.precog.common.json._
import com.precog.util.BitSet

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

import scala.collection.mutable

abstract class cleanup(f: File) extends After {
  def after = {
    try {
      f.delete()
    } catch {
      case _: Exception => ()
    }
  }
}

object RawHandlerSpecs extends Specification with ScalaCheck {
  def blockid = 999
  def tempfile(): File = {
    val f = File.createTempFile("niflheim", ".rawlog")
    f.delete()
    f
  }

  implicit val ordering = scala.math.Ordering.by[(String, CType), String](_.toString)

  def slurp(f: File): String =
    new java.util.Scanner(f, "UTF-8").useDelimiter("\0").next()

  def json(s: String): Seq[JValue] =
    JParser.parseManyFromString(s).valueOr(throw _)

  def testSnapshotKeys(s2: Segments, keys: Set[(String, CType)]) {
    s2.m.keys.map { case (a, b) => (a.toString, b) } must_== keys
  }

  def testArraySegment[A](s: Segments, id: Long, cp: CPath, ct: CType, tpls: List[(Int, A)]) {
    val seg = s.a(s.m((cp, ct))).asInstanceOf[ArraySegment[A]]
    tpls.foreach { case (i, v) =>
      (i, seg.defined.get(i)) must_== (i, true)
      (i, seg.values(i)) must_== (i, v)
    }
  }

  def testBooleanSegment[A](s: Segments, id: Long, cp: CPath, ct: CType, tpls: List[(Int, A)]) {
    val seg = s.a(s.m((cp, ct))).asInstanceOf[BooleanSegment]
    tpls.foreach { case (i, v) =>
      (i, seg.defined.get(i)) must_== (i, true)
      (i, seg.values.get(i)) must_== (i, v)
    }
  }

  def bitset(ns: Int*) = {
    val bs = new BitSet
    ns.foreach(bs.set)
    bs
  }

  def decs(ns: Double*): Array[BigDecimal] = ns.map(BigDecimal(_)).toArray

  def makeps(f: File) = new PrintStream(new FileOutputStream(f, true), false, "UTF-8")

  "raw handler" should {

    /**
     * Just tests the most basic functionality: generating snapshots
     * from repeated writes. This is the core of what RawHandler will
     * normally be doing.
     */
    val tmp1 = tempfile()
    "generate snapshots from writes" in new cleanup(tmp1) {
      val h = RawHandler.empty(blockid, tmp1)
      h.length must_== 0

      h.snapshot(None).length must_== 0

      h.write(16, json("""
{"a": 123, "b": true, "c": false, "d": null, "e": "cat", "f": {"aa": 11.0, "bb": 22.0}}
{"a": 9999.0, "b": "xyz", "arr": [1,2,3]}
{"a": 0, "b": false, "c": 0.0, "y": [], "z": {}}
"""))

      h.length must_== 3

      val segs1 = h.snapshot(None)
      segs1 must contain(ArraySegment(blockid, CPath(".a"), CNum, bitset(0, 1, 2), decs(123, 9999.0, 0)))
      segs1 must contain(BooleanSegment(blockid, CPath(".b"), bitset(0, 2), bitset(0), 3))

      h.write(17, json("""
999
123.0
"cat"
[1,2,3.0, "four"]
{"b": true}
"""))

      h.length must_== 8

      val segs2 = h.snapshot(None)
      segs2 must contain(BooleanSegment(blockid, CPath(".b"), bitset(0, 2, 7), bitset(0, 7), 8))
    }


    /**
     * Test log file writing/reading.
     *
     * The RawHandler should support being stopped and then recreated
     * on its previous log. This test doesn't test the format itself,
     * but just whether the data written can be read back in correctly.
     */
    val tmp2 = tempfile()
    "correctly read log files" in new cleanup(tmp2) {
      val h1 = RawHandler.empty(blockid, tmp2)

      val js = """
{"a": 123, "b": true, "c": false, "d": null, "e": "cat", "f": {"aa": 11.0, "bb": 22.0}}
{"a": 9999.0, "b": "xyz", "arr": [1,2,3]}
{"a": 0, "b": false, "c": 0.0, "y": [], "z": {}}
""".trim

      h1.write(18, json(js))
      val s1 = h1.snapshot(None)
      h1.close()

      val (h2, events, true) = RawHandler.load(blockid, tmp2)
      val s2 = h2.snapshot(None)

      implicit val ord: Ordering[Segment] = Ordering.by[Segment, String](_.toString)

      events.toSet must_== Set(18)
      s2.sorted must_== s1.sorted
    }


    /**
     * Test recovery from corrupted rawlog file.
     *
     * In this case the log is missing an "##end 101\n" stanza. The
     * first load() should clean up the file and report the error.
     * Future loads should load the same data without complaint
     * (indicating the file has been cleaned).
     */
    val tmp3 = tempfile()
    "recover from errors" in new cleanup(tmp3) {
    
      // write a valid event, then a mal-formed event
      val ps = makeps(tmp3)
      RawLoader.writeHeader(ps, blockid)
      RawLoader.writeEvents(ps, 100, json("""{"a": 1000, "b": 2.0}"""))
      ps.println("##start 101")
      ps.println("""{"a": 2000, "b": 3.0}""")
      ps.close()
    
      // try to load
      val (h1, events1, ok1) = RawHandler.load(blockid, tmp3)
      h1.length must_== 1
      events1.toSet must_== Set(100)
      ok1 must_== false
    
      // close this handler
      h1.close()
    
      // open a new handler, we should have sanitized the rawlog
      val (h2, events2, ok2) = RawHandler.load(blockid, tmp3)
      h2.length must_== 1
      events2.toSet must_== Set(100)
      ok2 must_== true
    }
    
    
    /**
     * Test missing files.
     *
     * In case the log is not there, load() must throw an Exception.
     */
    val tmp4 = tempfile()
    "throw an exception when loading empty logs" in new cleanup(tmp4) {
      RawHandler.load(blockid, tmp4) must throwA[Exception]
    }
    
    
    /**
     * Test file collisions.
     *
     * In case the log is already there, empty() must throw an Exception.
     */
    val tmp5 = tempfile()
    "throw an exception when creating already-present logs " in new cleanup(tmp5) {
      RawHandler.empty(blockid, tmp5).close()
      RawHandler.empty(blockid, tmp5) must throwA[Exception]
    }


    /**
     * Empty rawlog.
     *
     * It is fine to have a rawlog without any actual events in it.
     */
    val tmp6 = tempfile()
    "load empty files " in new cleanup(tmp6) {
      RawHandler.empty(blockid, tmp6).close()
      val (h, es, ok) = RawHandler.load(blockid, tmp6)
      ok must_== true
      es.isEmpty must_== true
      h.length must_== 0
    }
    
    
    /**
     * Test recovery from totally corrupted rawlog file #2.
     *
     * In this case the log is full of garbage; load() should throw an error.
     */
    val tmp7 = tempfile()
    "throw errors when totally corrupted" in new cleanup(tmp7) {
      val ps = makeps(tmp7)
      ps.println("jwgeigjewaijgweigjweijew")
      ps.close()
      RawHandler.load(blockid, tmp7) must throwA[Exception]
    }
    
    
    /**
     * Test recovery from partially-corrupted rawlog file #3.
     *
     * In this case the log is OK until an event ends up full of garbage.
     */
    val tmp8 = tempfile()
    "recover when partially corrupted" in new cleanup(tmp8) {
      val range = (0 until 20)
    
      val ps = makeps(tmp8)
      RawLoader.writeHeader(ps, blockid)
      def makejson(i: Int) = json("""{"a": %s, "b": %s}""" format (i * 2, i * 3))
      range.foreach(i => RawLoader.writeEvents(ps, i, makejson(i)))
      ps.println("biewjgwijgjigiwej")
      ps.close()
    
      val (h, events, ok) = RawHandler.load(blockid, tmp8)
      h.length must_== range.length
      events.toSet must_== range.toSet
      ok must_== false
    
      // double-check the data
      val cpa = CPath(".a")
      val cpb = CPath(".b")
      val bs = new BitSet()
      range.foreach(i => bs.set(i))
      
      val sa = ArraySegment(blockid, cpa, CNum, bs.copy, range.map(i => BigDecimal(i * 2)).toArray)
      val sb = ArraySegment(blockid, cpb, CNum, bs.copy, range.map(i => BigDecimal(i * 3)).toArray)

      h.snapshot(Some(Set(cpa))) must contain(sa)
      h.snapshot(Some(Set(cpb))) must contain(sb)
    }

  }
} 
