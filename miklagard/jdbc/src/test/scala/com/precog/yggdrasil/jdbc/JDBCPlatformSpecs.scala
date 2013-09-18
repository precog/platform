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
package jdbc

import akka.dispatch.{Await, Future, Promise}
import akka.util.Duration

import blueeyes.json._

import com.precog.bytecode._
import com.precog.common._
import com.precog.common.ingest._

import com.precog.common.security._
import com.precog.mimir._
import com.precog.muspelheim._
import com.precog.yggdrasil.actor.StandaloneShardSystemConfig
import com.precog.yggdrasil.util._
import com.precog.util.PrecogUnit

import com.weiglewilczek.slf4s.Logging

import java.io.File
import java.sql.DriverManager
import java.util.concurrent.{Executors, TimeUnit}

import org.specs2.specification.{Fragments, Step}

import scalaz._

object JDBCPlatformSpecEngine extends Logging {
  Class.forName("org.h2.Driver")

  def jvToSQL(jv: JValue): (String,String) = jv match {
    case JBool(v)      => ("BOOL", v.toString)
    case JNumLong(v)   => ("BIGINT", v.toString)
    case JNumDouble(v) => ("DOUBLE", v.toString)
    case JNumBigDec(v) => ("DECIMAL", v.toString)
    case JNumStr(v)    => ("DECIMAL", v)
    case JString(v)    => ("VARCHAR", "'" + v.replaceAll("'", "\\'") + "'")
    case _              => sys.error("SQL conversion doesn't support: " + jv)
  }

  private[this] val lock = new Object

  private[this] var refcount = 0

  private[this] val scheduler = Executors.newScheduledThreadPool(1)

  // This creates an in-memory DB that stays open forever
  val dbURL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"

  def shutdown() {
    val conn = DriverManager.getConnection(dbURL)
    val stmt = conn.createStatement
    stmt.execute("SHUTDOWN")
  }

  def acquire = lock.synchronized {
    refcount += 1

    if (refcount == 1) {
      logger.debug("Initializing fresh DB instance")
      runLoads()
      logger.debug("DB initialized")
    }

    logger.debug("DB acquired, refcount = " + refcount)
  }

  def release: Unit = lock.synchronized {
    refcount -= 1

    if (refcount == 0) {
      scheduler.schedule(checkUnused, 5, TimeUnit.SECONDS)
    }

    logger.debug("DB released, refcount = " + refcount)
  }

  val checkUnused = new Runnable {
    def run = lock.synchronized {
      logger.debug("Checking for unused JDBCPlatformSpecEngine. Count = " + refcount)
      if (refcount == 0) {
        logger.debug("Shutting down DB after final release")
        shutdown()
        logger.debug("DB shutdown complete")
      }
    }
  }

  def runLoads(): Unit = {
    logger.debug("Starting load")


    // Load the datasets into our test mongo by enumerating the test_data directory and loading each dataset found
    val dataDirURL = this.getClass.getClassLoader.getResource("test_data")

    if (dataDirURL == null || dataDirURL.getProtocol != "file") {
      logger.error("No data dir: " + dataDirURL)
      throw new Exception("Failed to locate test_data directory. Found: " + dataDirURL)
    }

    logger.debug("Loading from " + dataDirURL)


    def loadFile(path : String, file: File) {
      if (file.isDirectory) {
        file.listFiles.foreach { f =>
          logger.debug("Found child: " + f)
          loadFile(path + file.getName + "_", f)
        }
      } else {
        if (file.getName.endsWith(".json")) {
          try {
            val tableName = path + file.getName.replace(".json","")
            logger.debug("Loading %s into /test/%s".format(file, tableName))

            JParser.parseManyFromFile(file) match {
              case Success(data) =>
                val rows: Seq[Seq[(String, (String, String))]] = data.map { jv =>
                  jv.flattenWithPath.map { case (p, v) => (JDBCColumnarTableModule.escapePath(p.toString.drop(1)), jvToSQL(v)) }
                }

                // Two passes: first one constructs a schema for the table, second inserts data
                val schema = rows.foldLeft(Set[(String, String)]()) {
                  case (acc, properties) => acc ++ (properties.map { case (p, (t, _)) => (p, t) }).toSet
                }

                val ddlCreate = "CREATE TABLE %s (%s);".format(tableName, schema.map { case (p, t) => p + " " + t }.mkString(", "))

                logger.debug("Create = " + ddlCreate)

                val conn = DriverManager.getConnection(dbURL)

                try {
                  val stmt = conn.createStatement

                  stmt.executeUpdate(ddlCreate)


                  rows.foreach {
                    properties =>
                    val columns = properties.map(_._1).mkString(", ")
                    val values = properties.map(_._2._2).mkString(", ")

                    val insert = "INSERT INTO %s (%s) VALUES (%s);".format(tableName, columns, values)

                    logger.debug("Inserting with " + insert)

                    stmt.executeUpdate(insert)
                  }

                  stmt.close()
                } finally {
                  conn.close()
                }

                // Make sure we have the right amount of data
                val conn2 = DriverManager.getConnection(dbURL)
                val stmt2 = conn2.createStatement

                val rs = stmt2.executeQuery("SELECT COUNT(*) AS total FROM " + tableName)

                rs.next

                assert(rs.getLong("total") == rows.length)

                logger.debug(tableName + " has size " + rs.getLong("total"))

                stmt2.close()
                conn2.close()

              case Failure(error) => logger.error("Error loading: " + error)
            }
          } catch {
            case t: Throwable => logger.error("Error loading: " + t)
          }
        }
      }
    }

    (new File(dataDirURL.toURI)).listFiles.foreach { f =>
      loadFile("", f)
    }
  }
}

trait JDBCPlatformSpecs extends ParseEvalStackSpecs[Future]
    with JDBCColumnarTableModule
    with Logging
    with StringIdMemoryDatasetConsumer[Future] { self =>

  class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with EvaluatorConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig
      with JDBCColumnarTableModuleConfig

  object yggConfig extends YggConfig

  override def controlTimeout = Duration(10, "minutes")      // it's just unreasonable to run tests longer than this

  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(asyncContext, yggConfig.maxEvalDuration)

  val unescapeColumnNames = true

  val report = new LoggingQueryLogger[Future, instructions.Line]
      with ExceptionQueryLogger[Future, instructions.Line]
      with TimingQueryLogger[Future, instructions.Line] {

    implicit def M = self.M
  }

  trait TableCompanion extends JDBCColumnarTableCompanion

  object Table extends TableCompanion {
    import trans._

    val databaseMap = Map("test" -> JDBCPlatformSpecEngine.dbURL)

    override def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      // Rewrite paths of the form /foo/bar/baz to /test/foo_bar_baz
      val pathFixTS = Map1(Leaf(Source), CF1P("fix_paths") {
        case orig: StrColumn => new StrColumn {
          def apply(row: Int): String = {
            val newPath = "/test/" + orig(row).replaceAll("^/|/$", "").replace('/', '_')
            logger.debug("Fixed %s to %s".format(orig(row), newPath))
            newPath
          }
          def isDefinedAt(row: Int) = orig.isDefinedAt(row)
        }
      })
      val transformed = table.transform(pathFixTS)
      super.load(transformed, apiKey, tpe)
    }
  }

  def userMetadataView(apiKey: APIKey) = null

  def startup() {
    JDBCPlatformSpecEngine.acquire
  }

  def shutdown() {
    JDBCPlatformSpecEngine.release
  }

  override def map (fs: => Fragments): Fragments = Step { startup() } ^ fs ^ Step { shutdown() }

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future) =
    new Evaluator[N](N0)(mn,nm) {
      val report = new LoggingQueryLogger[N, instructions.Line]
          with ExceptionQueryLogger[N, instructions.Line]
          with TimingQueryLogger[N, instructions.Line] {

        val M = N0
      }
      class YggConfig extends EvaluatorConfig {
        val idSource = new FreshAtomicIdSource
        val maxSliceSize = 10
      }
      val yggConfig = new YggConfig
      def freshIdScanner = self.freshIdScanner
    }
}

/*
// These are disabled for now because getting SQL tables to hold our JSON datasets
// is too onerous at this point, and won't show us anything that the NIHDB and Mongo
// specs don't already

class JDBCBasicValidationSpecs extends BasicValidationSpecs with JDBCPlatformSpecs

class JDBCHelloQuirrelSpecs extends HelloQuirrelSpecs with JDBCPlatformSpecs

class JDBCLogisticRegressionSpecs extends LogisticRegressionSpecs with JDBCPlatformSpecs

class JDBCMiscStackSpecs extends MiscStackSpecs with JDBCPlatformSpecs

class JDBCRankSpecs extends RankSpecs with JDBCPlatformSpecs

class JDBCRenderStackSpecs extends RenderStackSpecs with JDBCPlatformSpecs

class JDBCUndefinedLiteralSpecs extends UndefinedLiteralSpecs with JDBCPlatformSpecs
*/

class JDBCLoadSpecs extends EvalStackSpecs with JDBCPlatformSpecs {
  "JDBC stack support" should {
    "count a filtered clicks dataset" in {
      val input = """
        | clicks := //clicks
        | count(clicks where clicks.time > 0)""".stripMargin

      eval(input) mustEqual Set(SDecimal(100))
    }

    "count the campaigns dataset" >> {
      "<root>" >> {
        eval("count(//campaigns)") mustEqual Set(SDecimal(100))
      }

      "gender" >> {
        eval("count((//campaigns).gender)") mustEqual Set(SDecimal(100))
      }

      "platform" >> {
        eval("count((//campaigns).platform)") mustEqual Set(SDecimal(100))
      }

      "campaign" >> {
        eval("count((//campaigns).campaign)") mustEqual Set(SDecimal(100))
      }

      "cpm" >> {
        eval("count((//campaigns).cpm)") mustEqual Set(SDecimal(100))
      }
    }

    "reduce the obnoxiously large dataset" >> {
      "<root>" >> {
        eval("mean((//obnoxious).v)") mustEqual Set(SDecimal(50000.5))
      }
    }
  }
}
