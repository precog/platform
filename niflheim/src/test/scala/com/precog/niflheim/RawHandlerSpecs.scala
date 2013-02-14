package com.precog.niflheim

import java.io._
import blueeyes.json._
import com.precog.common._
import com.precog.common.json._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

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

      val s1 = h.snapshot
      s1.id must_== blockid
      s1.length must_== 0
      s1.m.size must_== 0
      s1.a.size must_== 0

      h.write(16, json("""
{"a": 123, "b": true, "c": false, "d": null, "e": "cat", "f": {"aa": 11.0, "bb": 22.0}}
{"a": 9999.0, "b": "xyz", "arr": [1,2,3]}
{"a": 0, "b": false, "c": 0.0, "y": [], "z": {}}
"""))

      val s2 = h.snapshot
      s2.id must_== blockid
      s2.length must_== 3

      testSnapshotKeys(s2, Set(
          (".a", CNum), (".b", CBoolean), (".b", CString), (".c", CBoolean), (".c", CNum),
          (".d", CNull), (".e", CString), (".f.aa", CNum), (".f.bb", CNum),
          (".arr[0]", CNum), (".arr[1]", CNum), (".arr[2]", CNum),
          (".y", CEmptyArray), (".z", CEmptyObject)
      ))

      val tpla = List(
        (0, BigDecimal(123)),
        (1, BigDecimal(9999.0)),
        (2, BigDecimal(0))
      )

      testArraySegment(s2, blockid, CPath(".a"), CNum, tpla)
      testBooleanSegment(s2, blockid, CPath(".b"), CBoolean, List((0, true), (2, false)))

      h.write(17, json("""
999
123.0
"cat"
[1,2,3.0, "four"]
{"b": true}
"""))

      val s3 = h.snapshot
      s3.id must_== blockid
      s3.length must_== 8

      testSnapshotKeys(s3, Set(
          (".", CNum), (".", CNum), (".", CString),
          (".a", CNum), (".b", CBoolean), (".b", CString), (".c", CBoolean), (".c", CNum),
          (".d", CNull), (".e", CString), (".f.aa", CNum), (".f.bb", CNum),
          (".arr[0]", CNum), (".arr[1]", CNum), (".arr[2]", CNum),
          (".y", CEmptyArray), (".z", CEmptyObject),
          ("[0]", CNum), ("[1]", CNum), ("[2]", CNum), ("[3]", CString)
      ))
      
      testArraySegment(s3, blockid, CPath(".a"), CNum, tpla)
      testBooleanSegment(s3, blockid, CPath(".b"), CBoolean, List((0, true), (2, false), (7, true)))
      testArraySegment(s3, blockid, CPath("."), CNum, List((3, BigDecimal(999)), (4, BigDecimal(123.0))))
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
      val s1 = h1.snapshot()
      h1.close()

      val (h2, events, true) = RawHandler.load(blockid, tmp2)
      val s2 = h2.snapshot()

      events.toSet must_== Set(18)
      s2 must_== s1
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

  }
} 
