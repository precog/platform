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

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._

import scalaz.Need

import scala.collection.mutable

trait PrecogLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  val PrecogNamespace = Vector("precog")
  
  trait PrecogLib extends ColumnarTableLib {
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
        trans.Scan(
          trans.InnerArrayConcat(
            trans.WrapArray(left),
            trans.WrapArray(right)),
          new EnrichmentScanner(ctx))
      }
      
      class EnrichmentScanner(ctx: EvaluationContext) extends CScanner {
        type A = Unit
        
        def init = ()
        
        def scan(a: Unit, columns: Map[ColumnRef, Column], range: Range): M[(A, Map[ColumnRef, Column])] = {
          val urlsM = columns get ColumnRef(CPath.Identity \ 1 \ "url", CString)
          
          // TODO options
          urlsM map { urls =>
            val ranges: List[(String, Range)] = {
              var currentValue: String = null      // awesome sauce!!
              var currentHead = -1
              val ranges = new mutable.ListBuffer[(String, Range)]()
              
              RangeUtil.loopDefined(range, urls) { i =>
                if (urls(i) != currentValue) {
                  if (currentHead >= 0) {
                    ranges += (currentValue -> (currentHead until i))
                  }
                  
                  currentHead = i
                  currentValue = urls(i)
                }
              }
              
              if (currentHead >= 0) {
                ranges += (currentValue -> (currentHead to range.last))
              }
              
              ranges.toList
            }
            
            val targetColumns = columns collect {
              case (ColumnRef(CPath(CPathIndex(0), tail @ _*), tpe), col) =>
                ColumnRef(tail, tpe) -> col
            }
            
            val projectedTargets = ranges map {
              case (url, range) => {
                val projected = targetColumns mapValues { col =>
                  cf.util.RemapFilter(range.contains, range.head)(col).get
                }
                
                url -> (projected, range.length)
              }
            }
            
            val data = projectedTargets map {
              case (url, (columns0, size0)) => {
                val slice = new Slice {
                  val columns = columns0
                  val size = size0
                }
                
                val sliceJson = slice.renderJson[M](',')._1
                
                val prologue = """{"accountId":"%s","email":"%s","units":%d,"data":[""".format(
                  ctx.accountId,
                  ctx.email,
                  size0)
                  
                val prologueBuff = CharBuffer.allocate(prologue.length)
                prologueBuff.append(prologue)
                prologueBuff.flip()
                  
                val epilogue = "]}"
                
                val epilogueBuff = CharBuffer.allocate(epilogue.length)
                epilogueBuff.append(epilogue)
                epilogueBuff.flip()
                
                val stream = prologueBuff :: sliceJson ++ (epilogueBuff :: StreamT.empty[M, CharBuffer])
                
                url -> streamToString(stream)
              }
            }
            
            val requests = data map {
              case (address, dataM) => {
                (url(address).POST << data) OK as.String
              }
            }
            
            val results: List[Option[JValue]] = {
              val optJValues = requests map { future =>
                future map JsonParser.parseOpt
              }
              
              optJValues map { future =>
                future map { jv =>
                  jv \ "data"
                }
              }
              
              Future.sequence(optJValues)()     // the future is NOW
            }
            
            results
          }
        }
      }
      
      private def streamToString[N](stream: StreamT[N, CharBuffer]): N[String] =
        stream.toStream map { _.mkString }
    }
  }
}
