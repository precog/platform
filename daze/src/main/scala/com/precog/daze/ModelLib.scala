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

import util._

import yggdrasil._
import table._

import bytecode._

import common._

import scalaz._
import Scalaz._

trait ModelLibModule[M[+_]] {
  trait ModelSupport {
    trait ModelBase {
      case class Model(
        name: String,
        featureValues: Map[CPath, Double],
        constant: Double,
        resStdErr: Double,
        varCovar: Array[Array[Double]],
        degOfFreedom: Int)

      case class ModelSet(identity: Seq[Option[Long]], models: Set[Model])

      type Models = List[ModelSet]

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        private val kPath = CPath(TableModule.paths.Key)
        private val vPath = CPath(TableModule.paths.Value)

        private val coefficients = "coefficients"
        private val estimate = "estimate"
        private val residualStandardError = "residualStandardError"
        private val varianceCovariance = "varianceCovarianceMatrix"

        def reduce(schema: CSchema, range: Range): Models = {
          val rowIdentities: Int => Seq[Option[Long]] = {
            val indexedCols: Set[(Int, LongColumn)] = schema.columnRefs collect { 
              case ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), ctype) => 
                val idxCols = schema.columns(JObjectFixedT(Map("key" -> JArrayFixedT(Map(idx -> JNumberT)))))  
                assert(idxCols.size == 1)
                (idx, idxCols.head match {
                  case (col: LongColumn) => col
                  case _ => sys.error("expected LongColumn")
                })
            }

            val deref = indexedCols.toList.sortBy(_._1).map(_._2)
            (i: Int) => deref.map(c => c.isDefinedAt(i).option(c.apply(i)))
          }

          val rowModels: Int => Set[Model] = {
            val features: Map[String, Set[(String, CPath, CType)]] = {
              schema.columnRefs.collect { 
                case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(0), rest @ _*), ctype)
                  if rest.last == CPathField(`estimate`) => 
                    (modelName, path, ctype) 
              } groupBy { _._1 }
            }

            val varCovar: Map[String, Set[(String, CPath, CType)]] = {
              schema.columnRefs.collect { 
                case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`varianceCovariance`), _ @ _*), ctype) =>
                  (modelName, path, ctype) 
              } groupBy { _._1 }
            }

            val featureValues: Map[String, Set[Option[(CPath, DoubleColumn)]]] = features lazyMapValues {
              _ map {
                case (_, cpath, ctype) => {
                  val jtpe: Option[JType] = Schema.mkType(Seq((cpath, ctype)))
                  jtpe map { tpe => 
                    val res = schema.columns(tpe)
                    assert(res.size == 1)
                    (cpath, res.head match {
                      case (col: DoubleColumn) => col
                      case _ => sys.error("expected DoubleColumn")
                    })
                  }
                }
              }
            }

            val varCovarValues: Map[String, Set[Option[(CPath, DoubleColumn)]]] = varCovar lazyMapValues {
              _ map {
                case (_, cpath, ctype) => {
                  val jtpe: Option[JType] = Schema.mkType(Seq((cpath, ctype)))
                  jtpe map { tpe => 
                    val res = schema.columns(tpe)
                    assert(res.size == 1)
                    (cpath, res.head match {
                      case (col: DoubleColumn) => col
                      case _ => sys.error("expected DoubleColumn")
                    })
                  }
                }
              }
            }

            val constant: Map[String, (String, CPath, CType)] = {
              schema.columnRefs.collect { 
                case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(1), CPathField(`estimate`)), ctype) => 
                  (modelName, path, ctype) 
              } groupBy { _._1 } lazyMapValues { case set => 
                assert(set.size == 1)
                set.head
              }
            }

            val resStdErr: Map[String, (String, CPath, CType)] = {
              schema.columnRefs.collect { 
                case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`residualStandardError`), CPathField(`estimate`)), ctype) => 
                  (modelName, path, ctype) 
              } groupBy { _._1 } lazyMapValues { case set => 
                assert(set.size == 1)
                set.head
              }
            }

            val degrees: Map[String, (String, CPath, CType)] = {
              schema.columnRefs.collect { 
                case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`residualStandardError`), CPathField("degreesOfFreedom")), ctype) => 
                  (modelName, path, ctype) 
              } groupBy { _._1 } lazyMapValues { case set => 
                assert(set.size == 1)
                set.head
              }
            }

            val constantValue: Map[String, Option[DoubleColumn]] = constant lazyMapValues {
              case (_, cpath, ctype) => {
                val jtpe: Option[JType] = Schema.mkType(Seq((cpath, ctype)))
                jtpe map { tpe => 
                  val res = schema.columns(tpe)
                  assert(res.size == 1)
                  res.head match {
                    case (col: DoubleColumn) => col
                    case _ => sys.error("expected DoubleColumn")
                  }
                }
              }
            }

            //todo remove all this duplication!!!!!!!!
            //todo this is urgent; cannot have all this nonsense on master
            val resStdErrValue: Map[String, Option[DoubleColumn]] = resStdErr lazyMapValues {
              case (_, cpath, ctype) => {
                val jtpe: Option[JType] = Schema.mkType(Seq((cpath, ctype)))
                jtpe map { tpe => 
                  val res = schema.columns(tpe)
                  assert(res.size == 1)
                  res.head match {
                    case (col: DoubleColumn) => col
                    case _ => sys.error("expected DoubleColumn")
                  }
                }
              }
            }

            val degreesValue: Map[String, Option[DoubleColumn]] = degrees lazyMapValues {
              case (_, cpath, ctype) => {
                val jtpe: Option[JType] = Schema.mkType(Seq((cpath, ctype)))
                jtpe map { tpe => 
                  val res = schema.columns(tpe)
                  assert(res.size == 1)
                  res.head match {
                    case (col: DoubleColumn) => col
                    case _ => sys.error("expected DoubleColumn")
                  }
                }
              }
            }

            val featuresFinal: Map[String, Map[CPath, DoubleColumn]] = featureValues lazyMapValues {
              _ collect { case opt if opt.isDefined => opt.get } toMap
            }

            val constantFinal: Map[String, DoubleColumn] = constantValue collect {
              case (str, opt) if opt.isDefined => (str, opt.get)
            }

            val resStdErrFinal: Map[String, DoubleColumn] = resStdErrValue collect {
              case (str, opt) if opt.isDefined => (str, opt.get)
            }

            val degreesFinal: Map[String, DoubleColumn] = degreesValue collect {
              case (str, opt) if opt.isDefined => (str, opt.get)
            }

            val varCovarFinal: Map[String, Map[CPath, DoubleColumn]] = varCovarValues lazyMapValues {
              _ collect { case opt if opt.isDefined => opt.get } toMap
            }

            //todo better way to do intersection please
            val commonKeys = featuresFinal.keySet & constantFinal.keySet & resStdErrFinal.keySet & varCovarFinal.keySet & degreesFinal.keySet

            //todo does another case class kind of like Model make sense here, but for columns?
            val joined = commonKeys map { case field => 
              (field, (featuresFinal(field), constantFinal(field), resStdErrFinal(field), varCovarFinal(field), degreesFinal(field)))
            } toMap

            (i: Int) => joined collect { case (field, (values, constant, resStdErr, varCovar, degs)) if constant.isDefinedAt(i) => 
              val fts = values collect { case (CPath(TableModule.paths.Value, CPathField(_), CPathField(`coefficients`), CPathIndex(0), rest @ _*), col)
                if col.isDefinedAt(i) && rest.last == CPathField(`estimate`) =>
                  val paths = TableModule.paths.Value +: rest.take(rest.length - 1)
                  (CPath(paths: _*), col.apply(i))
              }

              val cnst = constant.apply(i)
              val rse = resStdErr.apply(i)
              val dof = degs.apply(i).toInt

              val vc: Map[CPath, Double] = varCovar collect { case (CPath(TableModule.paths.Value, CPathField(_), CPathField(`varianceCovariance`), rest @ _*), col)
                if col.isDefinedAt(i) =>
                  (CPath(rest: _*), col.apply(i))
              }

              val size = fts.size + 1
              val acc = Array.fill(size)(new Array[Double](size))

              vc foreach {
                case (CPath(CPathIndex(i), CPathIndex(j)), value) if (i < size) && (j < size) => acc(i)(j) = value
                case _ => sys.error("Incorrect CPath structure found.")
              }

              Model(field, fts, cnst, rse, acc, dof)
            } toSet
          }

          range.toList flatMap { i =>
            val models = rowModels(i)
            if (models.isEmpty)
              None
            else
              Some(ModelSet(rowIdentities(i), models))
          }
        }
      }
    }
  }
}
