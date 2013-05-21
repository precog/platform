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
package com.precog
package daze

import com.precog.bytecode._
import com.precog.common._
import com.precog.util._

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

import java.nio.CharBuffer

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import spire.math.Order

import scalaz.std.tuple._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz._

import scala.collection.mutable

trait PrecogLibModule[M[+_]] extends ColumnarTableLibModule[M] with TransSpecModule with HttpClientModule[M] {
  val PrecogNamespace = Vector("precog")
  
  trait PrecogLib extends ColumnarTableLib {
    import trans._

    override def _lib2 = super._lib2 ++ Set(Enrichment)

    object Enrichment extends Op2(PrecogNamespace, "enrichment") {
      
      val tpe = BinaryOperationType(
        JObjectUnfixedT,
        JUnionT(
          JObjectFixedT(
            Map(
              "url" -> JTextT,
              "options" -> JObjectUnfixedT)),
          JObjectFixedT(Map("url" -> JTextT))),
        JObjectUnfixedT)
      
      def spec[A <: SourceType](ctx: MorphContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = {
        trans.MapWith(
          trans.InnerArrayConcat(
            trans.WrapArray(left),
            trans.WrapArray(right)),
          new EnrichmentMapper(ctx))
      }

      class EnrichmentMapper(ctx: MorphContext) extends CMapperM[M] {
        import Extractor.Error

        private type Result[+A] = Validation[NonEmptyList[HttpClientError \/ Error], A]
        private val httpError: HttpClientError => HttpClientError \/ Error = -\/(_)
        private val jsonError: Error => HttpClientError \/ Error = \/-(_)

        private def addOrCreate(row: Int, unique: List[(Int, BitSet)], order: Order[Int]): Option[(Int, BitSet)] = {
          unique match {
            case (row0, defined) :: tail if order.eqv(row, row0) =>
              defined.set(row)
              None

            case Nil =>
              val defined = BitSetUtil.create()
              defined.set(row)
              Some((row, defined))

            case _ :: tail => addOrCreate(row, tail, order)
          }
        }

        private def concatenate(stream0: StreamT[M, CharBuffer]): M[String] = {
          val sb = new StringBuffer
          def build(stream: StreamT[M, CharBuffer]): M[String] = stream.uncons flatMap {
            case Some((head, tail)) =>
              sb.append(head.toString)
              build(tail)
            case None => M point sb.toString
          }
          build(stream0)
        }

        def map(columns: Map[ColumnRef, Column], range: Range): M[Map[ColumnRef, Column]] = {
          val slice = Slice(columns, range.end)
          val params = slice.deref(CPathIndex(1))
          val urls = params.columns.get(ColumnRef(CPath("url"), CString)) match {
            case Some(col: StrColumn) => col
            case _ => ArrayStrColumn.empty(0)
          }
          val values = slice.deref(CPathIndex(0))

          // A list of (row, definedness) pairs. The row is the first row with
          // a particular unique combo of params.
          val paramsOrder = params.order
          val chunks0: List[(Int, BitSet)] = range.foldLeft(List.empty[(Int, BitSet)]) {
            case (acc, row) if urls.isDefinedAt(row) =>
              addOrCreate(row, acc, paramsOrder) map { pair => pair :: acc } getOrElse acc
            case (acc, _) => acc
          }

          val options = params.deref(CPathField("options"))
          // TODO: Add these values to MorphContext.
          val account = ctx.evalContext.account
          val baseOpts = Map(jfield("accountId", account.accountId), jfield("email", account.email))
          val chunks: List[((String, Map[String, JValue]), BitSet)] = chunks0 map { case (row, members) =>
            val url = urls(row)
            val opts = options.toJValue(row) match {
              case JObject(elems) => elems ++ baseOpts
              case _ => baseOpts
            }
            ((url, opts), members)
          }

          val resultsM: M[List[Result[Slice]]] = chunks traverse { case ((url, opts), members) =>
            val chunkValues = values.redefineWith(members)
            val (stream, _) = chunkValues.renderJson[M](',')

            concatenate(stream) map ("[" + _ + "]") flatMap { data =>
              val fields = opts.mapValues(_.renderCompact) + ("data" -> data)
              val requestBody = fields map { case (field, value) =>
                JString(field).renderCompact + ":" + value
              } mkString ("{", ",", "}")

              def populate(data: List[JValue]): Validation[Error, Slice] = {
                if (data.size != members.cardinality) {
                  Failure(Error.invalid("Number of items returned does not match number sent."))
                } else {
                  def sparseStream(row: Int, xs: List[RValue]): Stream[RValue] = if (row < slice.size) {
                    if (members(row)) Stream.cons(xs.head, sparseStream(row + 1, xs.tail))
                    else Stream.cons(CUndefined, sparseStream(row + 1, xs))
                  } else Stream.empty

                  val sparseData = sparseStream(0, data map (RValue.fromJValue(_)))
                  Success(Slice.fromRValues(sparseData))
                }
              }

              val client = HttpClient(url)
              val request = Request(HttpMethod.POST, body = Some(Request.Body("application/json", requestBody)))

              client.execute(request).run map { responseE =>
                val validation = for {
                  response <- httpError <-: responseE.validation
                  body     <- httpError <-: response.ok.validation
                  json     <- jsonError <-: (Error.thrown(_)) <-: JParser.parseFromString(body)
                  data     <- jsonError <-: (json \ "data").validated[List[JValue]]
                  result   <- jsonError <-: populate(data)
                } yield result
                validation leftMap (NonEmptyList(_))
              }
            }
          }
          
          resultsM map (_.sequence: Result[List[Slice]]) flatMap {
            case Success(slices) =>
              val resultSlice = slices.foldLeft(Slice(Map.empty, slice.size))(_ zip _).columns
              M point resultSlice

            case Failure(errors) =>
              val messages = errors.toList map (_.fold({ httpError =>
                "Error making HTTP request: " + httpError.userMessage
              }, { jsonError =>
                "Error parsing JSON: " + jsonError.message
              }))
              val units: M[List[Unit]] = messages traverse (ctx.logger.error(_))
              units flatMap { _ =>
                ctx.logger.die() map { _ => Map.empty }
              }
          }
        }
      }
    }
  }
}
