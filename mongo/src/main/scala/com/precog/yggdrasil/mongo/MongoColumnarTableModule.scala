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
package mongo

import com.precog.common._

import com.precog.common.security._
import com.precog.bytecode._
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

import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import org.bson.types.ObjectId

import java.io.File
import java.util.SortedMap
import java.util.Comparator

import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import org.apache.commons.codec.binary.Hex

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
import scala.collection.immutable.Queue

import TableModule._

trait MongoColumnarTableModuleConfig {
}

trait MongoColumnarTableModule extends BlockStoreColumnarTableModule[Future] {
  type YggConfig <: IdSourceConfig with ColumnarTableModuleConfig with BlockStoreColumnarTableModuleConfig with MongoColumnarTableModuleConfig

  def includeIdField: Boolean

  trait MongoColumnarTableCompanion extends BlockStoreColumnarTableCompanion with Logging {
    def mongo: Mongo
    def dbAuthParams: Map[String, String]

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

    sealed trait LoadState
    case class InitialLoad(paths: List[Path]) extends LoadState
    case class InLoad(cursorGen: () => DBCursor, skip: Int, remainingPaths: List[Path]) extends LoadState

    def safeOp[A](nullMessage: String)(v: => A): Option[A] = try {
      Option(v) orElse { logger.error(nullMessage); None }
    } catch {
      case t: Throwable => logger.error("Failure during Mongo query: %s(%s)".format(t.getClass, t.getMessage)); None
    }

    def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      for {
        paths <- pathsM(table)
      } yield {
        Table(
          StreamT.unfoldM[Future, Slice, LoadState](InitialLoad(paths.toList)) {
            case InLoad(cursorGen, skip, remaining) =>
              M.point {
                val (slice, nextSkip) = makeSlice(cursorGen, skip)
                logger.trace("Running InLoad: fetched %d rows, next skip = %s".format(slice.size, nextSkip))
                Some(slice, nextSkip.map(InLoad(cursorGen, _, remaining)).getOrElse(InitialLoad(remaining)))
              }

            case InitialLoad(path :: xs) =>
              path.elements.toList match {
                case dbName :: collectionName :: Nil =>
                  logger.trace("Running InitialLoad")
                  M.point {
                    for {
                      db <- safeOp("Database " + dbName + " does not exist")(mongo.getDB(dbName)).flatMap { d =>
                        if (! d.isAuthenticated && dbAuthParams.contains(dbName)) {
                          logger.trace("Running auth setup for " + dbName)
                          dbAuthParams.get(dbName).map(_.split(':')) flatMap {
                            case Array(user, password) =>
                              if (d.authenticate(user, password.toCharArray)) {
                                Some(d)
                              } else {
                                logger.error("Authentication failed for database " + dbName); None
                              }

                            case invalid =>
                              logger.error("Invalid user:password for %s: \"%s\"".format(dbName, invalid.mkString(":"))); None
                          }
                        } else {
                          Some(d)
                        }
                      }
                      coll <- safeOp("Collection " + collectionName + " does not exist") {
                        logger.trace("Fetching collection: " + collectionName)
                        db.getCollection(collectionName)
                      }
                      slice <- safeOp("Invalid result in query") {
                        logger.trace("Getting data from " + coll)
                        val selector = jTypeToProperties(tpe, Set()).foldLeft(new BasicDBObject()) {
                          case (obj, path) => obj.append(path, 1)
                        }

                        val cursorGen = () => coll.find(new BasicDBObject(), selector)

                        val (slice, nextSkip) = makeSlice(cursorGen, 0)

                        logger.debug("Gen slice of size " + slice.size)
                        (slice, nextSkip.map(InLoad(cursorGen, _, xs)).getOrElse(InitialLoad(xs)))
                      }
                    } yield slice
                  }

                case err =>
                  sys.error("MongoDB path " + path.path + " does not have the form /dbName/collectionName; rollups not yet supported.")
              }

            case InitialLoad(Nil) =>
              M.point(None)
          },
          UnknownSize
        )
      }
    }

    def makeSlice(cursorGen: () => DBCursor, skip: Int): (Slice, Option[Int]) = {
      import TransSpecModule.paths._

      // Sort by _id always to mimic NIHDB
      val cursor = cursorGen().sort(new BasicDBObject("_id", 1)).skip(skip).limit(yggConfig.maxSliceSize + 1)
      val objects = cursor.toArray
      cursor.close()

      val (hasMore, size0, columns0) = buildColumns(objects)

      val slice = new Slice {
        val size = size0
        val columns = if (includeIdField) {
          columns0 get ColumnRef(Key \ 0, CString) map { idCol =>
            columns0 + (ColumnRef(Value \ CPathField("_id"), CString) -> idCol)
          } getOrElse columns0
        } else columns0
      }

      // FIXME: If cursor is empty the generated columns won't satisfy
      // sampleData.schema. This will cause the subsumption test in Slice#typed
      // to fail unless it allows for vacuous success

      val nextSkip = if (hasMore) {
        Some(skip + slice.size)
      } else {
        None
      }

      (slice, nextSkip)
    }

    private def buildColumns(dbObjs: java.util.List[DBObject]): (Boolean, Int, Map[ColumnRef, Column]) = {
      val sliceSize = dbObjs.size

      val acc = mutable.Map.empty[(List[CPathNode], CType), ArrayColumn[_]]

      def insertValue(rprefix: List[CPathNode], row: Int, value: Any) {
        value match {
          case null =>
            acc.getOrElseUpdate((rprefix, CNull), {
              MutableNullColumn.empty()
            }).asInstanceOf[MutableNullColumn].unsafeTap(_.update(row, true))

          case objId: ObjectId =>
            // TODO: We should ensure this matches up w/ BlueEyes exactly.
            val value = "ObjectId(\"" + Hex.encodeHexString(objId.toByteArray) + "\")"
            val col = acc.getOrElseUpdate((rprefix, CString), {
              ArrayStrColumn.empty(sliceSize)
            }).asInstanceOf[ArrayStrColumn]
            col.update(row, value)

          case str: String =>
            val col = acc.getOrElseUpdate((rprefix, CString), {
              ArrayStrColumn.empty(sliceSize)
            }).asInstanceOf[ArrayStrColumn]
            col.update(row, str)

          case num: java.lang.Integer =>
            val col = acc.getOrElseUpdate((rprefix, CLong), {
              ArrayLongColumn.empty(sliceSize)
            }).asInstanceOf[ArrayLongColumn]
            col.update(row, num.longValue)

          case num: java.lang.Long =>
            val col = acc.getOrElseUpdate((rprefix, CLong), {
              ArrayLongColumn.empty(sliceSize)
            }).asInstanceOf[ArrayLongColumn]
            col.update(row, num.longValue)

          case num: java.lang.Float =>
            val col = acc.getOrElseUpdate((rprefix, CDouble), {
              ArrayDoubleColumn.empty(sliceSize)
            }).asInstanceOf[ArrayDoubleColumn]
            col.update(row, num.doubleValue)

          case num: java.lang.Double =>
            val col = acc.getOrElseUpdate((rprefix, CDouble), {
              ArrayDoubleColumn.empty(sliceSize)
            }).asInstanceOf[ArrayDoubleColumn]
            col.update(row, num.doubleValue)

          case bool: java.lang.Boolean =>
            val col = acc.getOrElseUpdate((rprefix, CBoolean), {
              ArrayBoolColumn.empty()
            }).asInstanceOf[ArrayBoolColumn]
            col.update(row, bool.booleanValue)

          case array: java.util.ArrayList[_] if array.isEmpty =>
            val col = acc.getOrElseUpdate((rprefix, CEmptyArray), {
              MutableEmptyArrayColumn.empty()
            }).asInstanceOf[MutableEmptyArrayColumn]
            col.update(row, true)

          case array: java.util.ArrayList[_] =>
            val values = array.listIterator()
            while (values.hasNext) {
              val idx = values.nextIndex()
              val value = values.next()
              insertValue(CPathIndex(idx) :: rprefix, row, value)
            }

          case dbObj: DBObject =>
            val keys = dbObj.keySet()
            if (keys.isEmpty) {
              acc.getOrElseUpdate((rprefix, CEmptyObject), {
                MutableEmptyObjectColumn.empty()
              }).asInstanceOf[MutableEmptyObjectColumn].unsafeTap(_.update(row, true))
            } else {
              val keys = dbObj.keySet().iterator()
              while (keys.hasNext) {
                val k = keys.next()
                insertValue(CPathField(k) :: rprefix, row, dbObj.get(k))
              }
            }

          case date: java.util.Date =>
            val col = acc.getOrElseUpdate((rprefix, CDate), {
              ArrayDateColumn.empty(sliceSize)
            }).asInstanceOf[ArrayDateColumn]
            col.update(row, new DateTime(date))
        }
      }

      def insert(dbObj: DBObject, row: Int) = {
        import TransSpecModule.paths._

        if (dbObj.containsField("_id")) {
          insertValue(CPathIndex(0) :: Key :: Nil, row, dbObj.get("_id"))
        }

        val keys = dbObj.keySet().iterator()
        val valuePrefix = Value :: Nil
        while (keys.hasNext) {
          val key = keys.next()
          if (key != "_id")
            insertValue(CPathField(key) :: valuePrefix, row, dbObj.get(key))
        }
      }

      val dbObjIter = dbObjs.iterator()
      var row = 0
      while (dbObjIter.hasNext && row < yggConfig.maxSliceSize) {
        insert(dbObjIter.next(), row)
        row += 1
      }

      (dbObjIter.hasNext, row, acc.map({ case ((rprefix, cType), col) =>
        (ColumnRef(CPath(rprefix.reverse), cType), col)
      }).toMap)
    }
  }
}


// vim: set ts=4 sw=4 et:
