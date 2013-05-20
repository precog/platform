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
    val kPath = CPath(TableModule.paths.Key)
    val vPath = CPath(TableModule.paths.Value)

    val coefficients = "coefficients"
    val estimate = "estimate"
    val residualStandardError = "residualStandardError"
    val varianceCovariance = "varianceCovarianceMatrix"
    val degreesOfFreedom = "degreesOfFreedom"

    trait ModelBase extends RegressionSupport {
      type Model <: ModelLike

      case class ModelSet(identity: Array[Long], models: Set[Model])
      type Models = List[ModelSet]

      trait ModelLike {
        val featureValues: Map[CPath, Double]
        val constant: Double

      }

      object ModelLike {
        def included(model: Model, cols: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
          val featurePaths = model.featureValues.keySet

          val res = cols filter { case (ColumnRef(cpath, ctype), col) =>
            featurePaths.contains(cpath)
          }

          val resPaths = res map { case (ColumnRef(cpath, _), _) => cpath } toSet

          if (resPaths == featurePaths) res
          else Map.empty[ColumnRef, Column]
        }

        def idRes(cols: Map[ColumnRef, Column], modelSet: ModelSet): Map[ColumnRef, Column] = {
          val featureCols = cols collect {
            case (ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), ctype), col) => 
              val path = Seq(TableModule.paths.Key, CPathIndex(idx))
              (ColumnRef(CPath(path: _*), ctype), col)
            case c @ (ColumnRef(CPath(TableModule.paths.Key), _), _) => c
          }

          val shift = featureCols.size

          val modelCols: Map[ColumnRef, Column] = modelSet.identity.zipWithIndex map { case (id, idx) => 
            (ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx + shift)), CLong), Column.const(id))
          } toMap

          modelCols ++ featureCols
        }

        def defined(cols: Map[ColumnRef, Column], range: Range): BitSet = {
          val columns = cols map { case (_, col) => col }

          BitSetUtil.filteredRange(range) { i =>
            if (columns.isEmpty) false
            else columns.forall(_ isDefinedAt i)
          }
        }

        def filteredRange(cols: Map[ColumnRef, Column], range: Range) = range.filter(defined(cols, range).apply)

        case class ScannerPrelims(
          includedModel: Map[ColumnRef, Column],
          definedModel: BitSet,  
          cpaths: Seq[CPath],
          includedCols: Map[CPath, DoubleColumn],
          resultArray: Array[Double])

        def makePrelims(model: Model, cols: Map[ColumnRef, Column], range: Range, trans: Double => Double): ScannerPrelims = {
          val includedModel = included(model, cols)
          val definedModel = defined(includedModel, range)

          val cpaths = includedModel.map { case (ColumnRef(cpath, _), _) => cpath }.toSeq sorted
          val modelDoubles = cpaths map { model.featureValues(_) }

          val includedCols = includedModel.collect { case (ColumnRef(cpath, _), col: DoubleColumn) => (cpath, col) }.toMap

          val resultArray = filteredRange(includedModel, range).foldLeft( new Array[Double](range.end)) { case (arr, i) =>
            val includedDoubles = cpaths map { includedCols(_).apply(i) }

            if (modelDoubles.length == includedDoubles.length) {
              val res = dotProduct(modelDoubles.toArray, includedDoubles.toArray) + model.constant
              arr(i) = trans(res)
              arr
            } else {
              sys.error("Incorrect number of feature values.") 
            }
          }

          ScannerPrelims(includedModel, definedModel, cpaths, includedCols, resultArray)
        }
      }
    }

    def createRowIdentities(schema: CSchema): Int => Array[Long] = {
      val indexedCols: Set[(Int, LongColumn)] = schema.columnRefs collect { 
        case ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), ctype) => {
          val idxCols = schema.columns(JObjectFixedT(Map("key" -> JArrayFixedT(Map(idx -> JNumberT)))))  
          if (idxCols.size == 1) {
            (idx, idxCols.head match {
              case (col: LongColumn) => col
              case _ => sys.error("Expected LongColumn.")
            })
          } else {
            sys.error("Cannot have identity columns of more than one numeric type.")
          }
        }
      }

      val deref = indexedCols.toList.sortBy(_._1).map(_._2)
      (i: Int) => deref.collect { case col if col.isDefinedAt(i) => col.apply(i) }.toArray
    }

    def determineColumns(schema: CSchema, cpaths: Set[CPath]): Map[CPath, DoubleColumn] = {
      cpaths.map { cpath =>
        val jtpe = Schema.mkType(Seq(ColumnRef(cpath, CDouble)))

        val col = jtpe flatMap { tpe =>
          val res = schema.columns(tpe)

          if (res.length == 1) res.head match {
            case (col: DoubleColumn) => Some(col)
            case _ => sys.error("Expected DoubleColumn.")
          } else if (res.length == 0) {
            None
          } else {
            sys.error("Incorrect number of columns.")
          }
        }
        (cpath, col)
      }.collect { case (path, col) if col.isDefined =>
        (path, col.get)
      }.toMap
    }

    def alignWithModels(schema: CSchema, modelWithPaths: Map[String, Set[CPath]]): Map[String, Map[CPath, DoubleColumn]] = {
      modelWithPaths map { case (modelName, cpaths) =>
        (modelName, determineColumns(schema, cpaths))
      }
    }

    trait LinearModelBase extends ModelBase {
      case class Model(
        name: String,
        featureValues: Map[CPath, Double],
        constant: Double,
        resStdErr: Double,
        varCovar: Array[Array[Double]],
        degOfFreedom: Int) extends ModelLike

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        def reduce(schema: CSchema, range: Range): Models = {
          val rowIdentities = createRowIdentities(schema)

          val modelNames: Set[String] = schema.columnRefs.collect {
            case ColumnRef(CPath(TableModule.paths.Value, CPathField(modelName), _ @ _*), _) => modelName
          }.toSet

          val interceptPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(1), CPathField(`estimate`))))
          }.toMap

          val stdErrPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`residualStandardError`), CPathField(`estimate`))))
          }.toMap

          val dofPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`residualStandardError`), CPathField(`degreesOfFreedom`))))
          }.toMap

          val covarPaths = schema.columnRefs.collect {
            case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`varianceCovariance`), _ @ _*), _) => (modelName, path)
          }.groupBy(_._1) map { case (modelName, paths) =>
            (modelName, paths.map(_._2))
          }
          
          val featuresPaths = schema.columnRefs.collect {
            case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(0), rest @ _*), _)
              if rest.length > 0 && rest.last == CPathField(`estimate`) => (modelName, path)
          }.groupBy(_._1) map { case (modelName, paths) =>
            (modelName, paths.map(_._2))
          }

          val interceptCols = alignWithModels(schema, interceptPaths)
          val stdErrCols = alignWithModels(schema, stdErrPaths)
          val dofCols = alignWithModels(schema, dofPaths)
          val covarCols = alignWithModels(schema, covarPaths)
          val featuresCols = alignWithModels(schema, featuresPaths)

          //error prone; ideally determine common keys earlier
          val commonKeys = interceptCols.keySet & stdErrCols.keySet & dofCols.keySet & covarCols.keySet & featuresCols.keySet

          val joined0 = commonKeys map { case field => 
            (field, List(interceptCols(field), stdErrCols(field), dofCols(field), covarCols(field), featuresCols(field)))
          } toMap

          val rowModels: Int => Set[Model] = (i: Int) => { 
            val joined = joined0 filterNot { case (_, cols) =>
              val definedCols = cols map { _ filter { case (_, col) => col.isDefinedAt(i) } }
              definedCols.exists(_.isEmpty)
            }
            
            joined.collect { case (field, cols @ List(constant, resStdErr, degs, varCovar, values)) =>
              val cnst = constant.map { case (_, col) =>
                col.apply(i)
              }.headOption getOrElse { sys.error("Constant term must exist") }

              val rse = resStdErr.map { case (_, col) =>
                col.apply(i)
              }.headOption getOrElse { sys.error("Error term must exist") }

              val dof = degs.map { case (_, col) =>
                col.apply(i).toInt
              }.headOption getOrElse { sys.error("DOF term must exist") }

              val fts = values map { case (CPath(TableModule.paths.Value, CPathField(_), CPathField(`coefficients`), CPathIndex(0), rest @ _*), col) =>
                val paths = TableModule.paths.Value +: rest.take(rest.length - 1)
                (CPath(paths: _*), col.apply(i))
              }

              val vc: Map[CPath, Double] = varCovar map { case (CPath(TableModule.paths.Value, CPathField(_), CPathField(`varianceCovariance`), rest @ _*), col) =>
                (CPath(rest: _*), col.apply(i))
              }
              val size = fts.size + 1
              val acc = Array.fill(size)(new Array[Double](size))

              vc foreach {
                case (CPath(CPathIndex(i), CPathIndex(j)), value) if (i < size) && (j < size) => acc(i)(j) = value
                case _ => sys.error("Incorrect CPath structure found.")
              }

              Model(field, fts, cnst, rse, acc, dof)
            }.toSet
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

    trait LogisticModelBase extends ModelBase {
      case class Model(
        name: String,
        featureValues: Map[CPath, Double],
        constant: Double) extends ModelLike

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        def reduce(schema: CSchema, range: Range): Models = {
          val rowIdentities = createRowIdentities(schema)

          val modelNames: Set[String] = schema.columnRefs.collect {
            case ColumnRef(CPath(TableModule.paths.Value, CPathField(modelName), _ @ _*), _) => modelName
          }.toSet

          val interceptPaths = modelNames.map { modelName =>
            (modelName, Set(CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(1), CPathField(`estimate`))))
          }.toMap

          val featuresPaths = schema.columnRefs.collect {
            case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(`coefficients`), CPathIndex(0), rest @ _*), _)
              if rest.length > 0 && rest.last == CPathField(`estimate`) => (modelName, path)
          }.groupBy(_._1) map { case (modelName, paths) =>
            (modelName, paths.map(_._2))
          }

          val interceptCols = alignWithModels(schema, interceptPaths)
          val featuresCols = alignWithModels(schema, featuresPaths)

          //error prone; ideally determine common keys earlier
          val commonKeys = interceptCols.keySet & featuresCols.keySet

          val joined0 = commonKeys map { case field => 
            (field, List(interceptCols(field), featuresCols(field)))
          } toMap

          val rowModels: Int => Set[Model] = (i: Int) => {
            val joined = joined0 filterNot { case (_, cols) =>
              val definedCols = cols map { _ filter { case (_, col) => col.isDefinedAt(i) } }
              definedCols.exists(_.isEmpty)
            }

            joined.collect { case (field, cols @ List(constant, values)) =>
              val cnst = constant.map { case (_, col) =>
                col.apply(i)
              }.headOption getOrElse { sys.error("Constant term must exist") }

              val fts = values collect { case (CPath(TableModule.paths.Value, CPathField(_), CPathField(`coefficients`), CPathIndex(0), rest @ _*), col) if col.isDefinedAt(i) =>
                val paths = TableModule.paths.Value +: rest.take(rest.length - 1)
                (CPath(paths: _*), col.apply(i))
              }

              Model(field, fts, cnst)
            }.toSet
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
