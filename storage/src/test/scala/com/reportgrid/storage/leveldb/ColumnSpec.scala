package com.reportgrid.storage.leveldb

import java.io.File

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.{BeforeAfter,Specification}
import org.specs2.specification.Scope
import Bijection._

import com.weiglewilczek.slf4s.Logging

class ColumnSpec extends Specification with ScalaCheck with ThrownMessages with Logging {
  trait columnSetup extends Scope with BeforeAfter {
    val dataDir = File.createTempFile("ColumnSpec", ".db")
    logger.info("Using %s for dbtest".format(dataDir))
    def before = dataDir.delete() // Ugly, but it works
    def after = {
      // Here we need to remove the entire directory and contents
      def delDir (dir : File) {
        dir.listFiles.foreach {
          case d if d.isDirectory => delDir(d)
          case f => f.delete()
        }
        dir.delete()
      }
      delDir(dataDir)
    }
  }

  "Columns" should {
    "Fail to create a new column without a provided comparator" in new columnSetup {
      LevelDBProjection(dataDir).isFailure must_== true
    }

    "Create a new column with a provided comparator" in new columnSetup {
      val c = LevelDBProjection(dataDir, Some(ProjectionComparator.Long))
      c.isSuccess must_== true
      c.map(_.close.unsafePerformIO)
    }

    "Open an existing column with a restored comparator" in new columnSetup {
      val initial = LevelDBProjection(dataDir, Some(ProjectionComparator.Long))
      initial.isSuccess must_== true
      initial.map(_.close.unsafePerformIO).flatMap(_ => LevelDBProjection(dataDir)).isSuccess must_== true
    }


//    "Properly persist ID lists" in {
//      check { (v : BigDecimal, ids : List[Long]) =>
//        ids.foreach(c.insert(_, v.underlying))
//        
//        val resultIds = c.getIds(v.underlying)
//
//        resultIds.size must_== ids.size
//      }
//    }
  }
}
