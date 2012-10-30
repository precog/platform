package com.precog.yggdrasil
package jdbm3

import com.google.common.io.Files
import com.weiglewilczek.slf4s.Logging

import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.specs2._
import org.specs2.execute.Result
import org.specs2.mutable.{After, Specification}
import org.scalacheck.{Arbitrary,Gen}

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import com.precog.common.{Path,VectorCase}
import com.precog.common.json._

import com.precog.util.IOUtils

import java.io.File

class JDBMProjectionSpec extends Specification with ScalaCheck with Logging with CValueGenerators {
  import Gen._
  import Arbitrary._

  override def defaultValues = super.defaultValues + (minTestsOk -> 20)

  case class ProjectionData(desc: ProjectionDescriptor, data: List[Seq[CValue]])

  implicit val genData: Arbitrary[ProjectionData] = Arbitrary(
    for {
      size       <- chooseNum(1,1000)//00) TODO bump back up to 100000 when memory isn't an issue.
      width      <- chooseNum(1,20) // TODO bump back up to 40 when bytebuffer overflows handled.
      types      <- listOfN(width, genCType)
      descriptor <- ProjectionDescriptor(1, types.toList.map { tpe => ColumnDescriptor(Path("/test"), CPath.Identity, tpe, Authorities(Set.empty)) })
      val typeGens: Seq[Gen[CValue]] = types.map(genCValue)
      data       <- genColumn(size, sequence[Array, CValue](typeGens))
    } yield ProjectionData(descriptor, data)
  )

  trait WithTempDir extends After {
    val tmpDir = IOUtils.createTmpDir("JDBMSpec").unsafePerformIO

    def after = {
      IOUtils.recursiveDelete(tmpDir).unsafePerformIO
    }
  }

  def readWriteColumn(pd: ProjectionData, baseDir: File): Result = {
    sys.error("Refactor")
    /*
    val ProjectionData(descriptor: ProjectionDescriptor, dataRaw: List[Seq[CValue]]) = pd
    val data = dataRaw.toArray
    logger.info("Running projection read/write spec of size " + data.length)
    logger.debug("Projection data writes to " + baseDir)

    try {
      val proj = new JDBMProjection(baseDir, descriptor){}

      logger.debug("New projection open")

      // Insert all data
      (0 until data.length).foreach {
        i => proj.insert(VectorCase(i), data(i), i % 100000 == 0).unsafePerformIO
      }
      
      proj.close()
    } catch {
      case t: Throwable => logger.error("Error writing projection data", t); throw t
    }

    try {
      val proj2 = new JDBMProjection(baseDir, descriptor, Int.MaxValue){}

      val read : Seq[JValue] = sys.error("todo")

      proj2.close()

      IOUtils.recursiveDelete(baseDir).unsafePerformIO

      forall(read.zipWithIndex) {
        case ((ids, v), i) => {
          ids mustEqual Array(i)
          v   mustEqual data(i)
        }
      }

      read.size mustEqual data.length
    } catch {
      case t: Throwable => logger.error("Error reading projection data"); throw t
    }
    */
  }

/*
  "JDBMProjections" should {
    "properly serialize and deserialize arbitrary columns" in new WithTempDir {
      check {
        pd: ProjectionData => readWriteColumn(pd, tmpDir)
      }
    }.pendingUntilFixed

    val indexGen = new java.util.Random()

    "properly serialize and deserialize columns with undefined values" in new WithTempDir {
      check {
        pd: ProjectionData => {
          val holeyData = pd.data.map {
            rowData => {
              val newRow = rowData.toArray
              val replaceIdx = indexGen.nextInt(newRow.length)
              newRow(replaceIdx) = CUndefined
              newRow.toSeq
            }
          }
          readWriteColumn(pd.copy(data = holeyData), tmpDir)
        }
      }
    }.pendingUntilFixed
  }
  */
}
