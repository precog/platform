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

import com.precog.bytecode._
import com.precog.common._

import com.precog.common.security._
import com.precog.yggdrasil.util._
import com.precog.util._
import Schema._
import metadata._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import akka.dispatch.Future

import blueeyes.json._
import blueeyes.persistence.mongo.json._
import BijectionsMongoJson._

import com.weiglewilczek.slf4s.Logging

import java.io.File
import java.util.SortedMap
import java.util.Comparator
import java.sql._

import org.joda.time.DateTime

import org.postgresql.util.PGobject

import scalaz._
import scalaz.Ordering._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.id._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

import TableModule._

trait JDBCColumnarTableModuleConfig {
}

object JDBCColumnarTableModule {
  def escapePath(path: String) = path.toList.map {
    case '['                => "PCLBRACKET"
    case ']'                => "PCRBRACKET"
    case '-'                => "PCFIELDDASH"
    case '.'                => "PCDOTSEP"
    case ' '                => "PCSPACE"
    case c if c.isUpper     => "PCUPPER" + c
    case c                  => c.toString
  }.mkString("")

  def unescapePath(name: String): String = {
    val initial = name.replace("PCLBRACKET", "[").replace("PCRBRACKET", "]").replace("PCFIELDDASH", "-").replace("PCDOTSEP", ".").replace("PCSPACE", " ")

    val parts = initial.split("PCUPPER")

    if (parts.length > 1) {
      parts.head.toLowerCase + parts.tail.map { ucSeg =>
        val (ucChar, rest) = ucSeg.splitAt(1)
        ucChar.toUpperCase + rest.toLowerCase
      }.mkString("")
    } else {
      parts.head.toLowerCase
    }
  }
}


trait JDBCColumnarTableModule
    extends BlockStoreColumnarTableModule[Future] {
  import JDBCColumnarTableModule._

  type YggConfig <: IdSourceConfig with ColumnarTableModuleConfig with BlockStoreColumnarTableModuleConfig with JDBCColumnarTableModuleConfig

  trait DBColumns {
    def extract(rs: ResultSet, rowId: Int): Unit

    // asPairs is intended to be called once reads are complete
    def columns: Seq[(ColumnRef, Column)]
  }

  case object EmptyDBColumn extends DBColumns {
    def extract(rs: ResultSet, rowId: Int) { }
    def columns = Seq.empty
  }

  case class SingleDBColumn(cref: ColumnRef, column: Column, extractor: (ResultSet, Int) => Unit) extends DBColumns {
    def extract(rs: ResultSet, rowId: Int) = extractor(rs, rowId)
    def columns = Seq(cref -> column)
  }

  private def notNull(rs: ResultSet, columnIndex: Int) = rs.getObject(columnIndex) != null

  protected def unescapeColumnNames: Boolean

  private def truncateString(input: String) =
    if (input.length > 43) {
      input.take(40) + "..."
    } else {
      input
    }

  private def metaToColumn(meta: ResultSetMetaData, index: Int): DBColumns = {
    val columnName = meta.getColumnLabel(index)
    val selector = paths.Value \ CPath(if (unescapeColumnNames) unescapePath(columnName) else columnName)

    import Types._

    meta.getColumnType(index) match {
      case BIT | BOOLEAN         =>
        val column = ArrayBoolColumn.empty
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getBoolean(index)) }
        SingleDBColumn(ColumnRef(selector, CBoolean), column, update)

      case CHAR | LONGNVARCHAR | LONGVARCHAR | NCHAR | NVARCHAR | VARCHAR =>
        val column = ArrayStrColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getString(index)) }
        SingleDBColumn(ColumnRef(selector, CString), column, update)

      case TINYINT               =>
        val column = ArrayLongColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getByte(index)) }
        SingleDBColumn(ColumnRef(selector, CLong), column, update)

      case SMALLINT               =>
        val column = ArrayLongColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getShort(index)) }
        SingleDBColumn(ColumnRef(selector, CLong), column, update)

      case INTEGER               =>
        val column = ArrayLongColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getInt(index)) }
        SingleDBColumn(ColumnRef(selector, CLong), column, update)

      case BIGINT                =>
        val column = ArrayLongColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getLong(index)) }
        SingleDBColumn(ColumnRef(selector, CLong), column, update)

      case REAL                  =>
        val column = ArrayDoubleColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getFloat(index)) }
        SingleDBColumn(ColumnRef(selector, CDouble), column, update)

      case DOUBLE | FLOAT        =>
        val column = ArrayDoubleColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getDouble(index)) }
        SingleDBColumn(ColumnRef(selector, CDouble), column, update)

      case DECIMAL | NUMERIC     =>
        val column = ArrayNumColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, rs.getBigDecimal(index)) }
        SingleDBColumn(ColumnRef(selector, CNum), column, update)

      case DATE           =>
        val column = ArrayDateColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, new DateTime(rs.getDate(index).getTime)) }
        SingleDBColumn(ColumnRef(selector, CDate), column, update)

      case TIMESTAMP      =>
        val column = ArrayDateColumn.empty(yggConfig.maxSliceSize)
        val update = (rs: ResultSet, rowId: Int) => if (notNull(rs, index)) { column.update(rowId, new DateTime(rs.getTimestamp(index).getTime)) }
        SingleDBColumn(ColumnRef(selector, CDate), column, update)

      case OTHER          =>
        // Here's where things get tricky. We support postgresql for now, but this code needs changed if we want to support something else
        if (meta.getClass.toString.contains("postgresql")) {
          new DBColumns {
            private[this] var buildColumns = Map.empty[ColumnRef, ArrayColumn[_]]

            def columns = buildColumns.toSeq

            def extract(rs: ResultSet, rowId: Int) = rs.getObject(index) match {
              case pgo: PGobject => pgo.getType match {
                case "hstore" =>
                  pgo.getValue.split(",|=>").toList.map { v => val t = v.trim; t.substring(1, t.length - 1) }.grouped(2).foreach {
                    case List(key, value) =>
                      val hsRef = ColumnRef(selector \ key, CString)
                      val column = buildColumns.getOrElse(hsRef, ArrayStrColumn.empty(yggConfig.maxSliceSize)).asInstanceOf[ArrayStrColumn].unsafeTap { c => c.update(rowId, value) }
                      buildColumns += (hsRef -> column)

                    case invalid => logger.error("Invalid pair in hstore value: " + invalid)
                  }

                case "json"   =>
                  JParser.parseFromString(pgo.getValue) match {
                    case Success(jv) =>
                      buildColumns = Slice.withIdsAndValues(jv, buildColumns, rowId, yggConfig.maxSliceSize, Some(selector \ CPath(_)))

                    case Failure(error) =>
                      logger.error("Failure parsing JSON column value (%s): %s".format(truncateString(pgo.getValue), error.getMessage))
                  }

                case other    =>
                  logger.warn("Unsupportd PostgreSQL type: " + other)
              }

              case other         =>
                logger.warn("Encountered unknown data from PostgreSQL: %s (%s)".format(other, other.getClass))
            }

          }
        } else {
          EmptyDBColumn
        }

      case other => logger.warn("Unsupported JDBC column type %d for %s".format(other, selector)); EmptyDBColumn
    }
  }

  private def columnsForResultSet(rs: ResultSet): Seq[DBColumns] = {
    val metadata = rs.getMetaData

    import java.sql.Types._

    (1 to metadata.getColumnCount).map(metaToColumn(metadata, _))
  }

  trait JDBCColumnarTableCompanion extends BlockStoreColumnarTableCompanion with Logging {
    /** Maps a given database name to a JDBC connection URL */
    def databaseMap: Map[String, String]

    private def jTypeToProperties(tpe: JType, current: Set[String]) : Set[String] = tpe match {
      case JArrayFixedT(elements) if current.nonEmpty => elements.map {
        case (index, childType) =>
          val newPaths = current.map { s => s + "[" + index + "]" }
          jTypeToProperties(childType, newPaths)
      }.toSet.flatten

      case JObjectFixedT(fields)                      => fields.map {
        case (name, childType) =>
          val newPaths = if (current.nonEmpty) {
            current.map { s => s + "." + name }
          } else {
            Set(name)
          }
          jTypeToProperties(childType, newPaths)
      }.toSet.flatten

      case _                                          => current
    }

    case class Query(expr: String, limit: Int) {
      private val baseQuery = if (limit > 0) { expr + " LIMIT " + limit } else { expr }

      def atOffset(offset: Long) = if (offset > 0) { baseQuery + " OFFSET " + offset } else baseQuery
    }

    sealed trait LoadState
    case class InitialLoad(paths: List[Path]) extends LoadState
    case class InLoad(connGen: () => Connection, query: Query, skip: Int, remainingPaths: List[Path]) extends LoadState

    def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      for {
        paths <- pathsM(table)
      } yield {
        import trans._
        val idSpec = InnerObjectConcat(Leaf(Source), WrapObject(WrapArray(Scan(Leaf(Source), freshIdScanner)), TransSpecModule.paths.Key.name))

        Table(
          StreamT.unfoldM[Future, Slice, LoadState](InitialLoad(paths.toList)) {
            case InLoad(connGen, query, skip, remaining) =>
              M.point {
                val (slice, nextSkip) = makeSlice(connGen, query, skip)
                Some((slice, nextSkip.map(InLoad(connGen, query, _, remaining)).getOrElse(InitialLoad(remaining))))
              }

            case InitialLoad(path :: xs) =>
              path.elements.toList match {
                case dbName :: tableName :: Nil =>
                  M.point {
                    try {
                      databaseMap.get(dbName).map { url =>
                        // Extra split/take at the end is because we can only map to column level. While hstore and json column types
                        // will end up with deepeer paths, they cannot be queried against in PostgreSQL
                        val columns = jTypeToProperties(tpe, Set()).map { c => c.split('.').head }
                        val query = Query("SELECT %s FROM %s".format(if (columns.isEmpty) "*" else columns.mkString(","), tableName), yggConfig.maxSliceSize)

                        logger.debug("Running query: " + query)

                        val connGen = () => DriverManager.getConnection(url)

                        val (slice, nextSkip) = makeSlice(connGen, query, 0)
                        Some(slice, nextSkip.map(InLoad(connGen, query, _, xs)).getOrElse(InitialLoad(xs)))
                      } getOrElse {
                        throw new Exception("Database %s is not configured" format dbName)
                      }
                    } catch {
                      case t =>
                        logger.error("Failure during JDBC query: " + t.getMessage)
                        // FIXME: We should be able to throw here and terminate the query, but something in BlueEyes is hanging when we do so
                        //throw new Exception("Failure during JDBC query: " + t.getMessage)
                        None
                    }
                  }

                case err =>
                  sys.error("JDBC path " + path.path + " does not have the form /dbName/tableName; rollups not yet supported.")
              }

            case InitialLoad(Nil) =>
              M.point(None)
          },
          UnknownSize
        ).transform(idSpec)//.printer("JDBC Table")
      }
    }

    def makeSlice(connGen: () => Connection, query: Query, skip: Int): (Slice, Option[Int]) = {
      import TransSpecModule.paths._

      try {
        val conn = connGen()

        try {
          // We could probably be slightly more efficient with driver-specific prepared statements for offset/limit
          val results = conn.createStatement.executeQuery(query.atOffset(skip))

          // Two words: ug ly
          val valColumns = columnsForResultSet(results)

          var rowIndex = 0

          while (results.next && rowIndex < yggConfig.maxSliceSize) {
            valColumns.foreach { dbc => dbc.extract(results, rowIndex) }

            rowIndex += 1
          }

          val slice = new Slice {
            val size = rowIndex
            val columns = valColumns.flatMap(_.columns).toMap
          }

          val nextSkip = if (rowIndex == yggConfig.maxSliceSize) {
            Some(skip + yggConfig.maxSliceSize)
          } else {
            None
          }

          (slice, nextSkip)
        } finally {
          conn.close()
        }
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
