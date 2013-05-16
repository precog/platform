package com.precog
package daze

import com.precog.bytecode._
import com.precog.common._
import com.precog.util._

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

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
      
      def spec[A <: SourceType](ctx: EvaluationContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = {
        trans.MapWith(
          trans.InnerArrayConcat(
            trans.WrapArray(left),
            trans.WrapArray(right)),
          new EnrichmentMapper(ctx))
      }
      
      class EnrichmentMapper(ctx: EvaluationContext) extends CMapperM[M] {
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
          // TODO: Add these values to EvaluationContext.
          // val baseOpts = Map(jfield("accountId", ctx.accountId), jfield("email", ctx.email))
          val baseOpts = Map.empty[String, JValue]
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
            val data = "[" + chunkValues.renderJson[M](',') + "]"
            val fields = opts.mapValues(_.renderCompact) + ("data" -> data)
            val requestBody = fields map { case (field, value) =>
              JString(field).renderCompact + ":" + value
            } mkString ("{", ",", "}")

            def populate(data: List[JValue]): Validation[Error, Slice] = {
              if (data != members.cardinality) {
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
          
          resultsM map (_.sequence: Result[List[Slice]]) map {
            case Success(slices) =>
              slices.foldLeft(Slice(Map.empty, slice.size))(_ zip _).columns
            case Failure(errors) =>
              sys.error("!!!!!!!!!!!!!!!!!!!!!")
          }
        }
      }
    }
  }
}
