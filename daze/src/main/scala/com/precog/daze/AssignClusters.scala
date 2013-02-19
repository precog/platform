package com.precog
package daze

import util._

import yggdrasil._
import table._

import bytecode._

import com.precog.common._
import common.json._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse._

import spire.implicits._

trait AssignClusterModule[M[+_]] extends ColumnarTableLibModule[M] {
  import trans._

  trait AssignClusterSupport extends ColumnarTableLib { 
    trait AssignClusterBase { self: Morphism2 =>
      case class ModelCluster(name: ClusterId, featureValues: Map[CPath, Double])
      case class Model(name: ModelId, clusters: Array[ModelCluster])
      case class ModelSet(identity: Seq[Option[Long]], models: Set[Model])
      type Models = List[ModelSet]

      private type ClusterId = String
      private type ModelId = String

      //val monoid = implicitly[Monoid[Models]]

      protected val reducer: CReducer[Models] = new CReducer[Models] {
        private val kPath = CPath(TableModule.paths.Key)
        private val vPath = CPath(TableModule.paths.Value)

        def reduce(schema: CSchema, range: Range): Models = {

          val rowIdentities: Int => Seq[Option[Long]] = {
            val indexedCols: Set[(Int, LongColumn)] = schema.columnRefs collect { 
              case ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), ctype) => 
                val idxCols = schema.columns(JObjectFixedT(Map("key" -> JArrayFixedT(Map(idx -> JNumberT)))))  
                assert(idxCols.size == 1)
                (idx, idxCols.head match { 
                  case (col: LongColumn) => col
                  case _ => sys.error("key column must be a LongColumn")
                })
            }

            val deref = indexedCols.toList.sortBy(_._1).map(_._2)
            (i: Int) => deref.map(c => c.isDefinedAt(i).option(c.apply(i)))
          }

          val rowModels: Int => Set[Model] = {
            val modelTuples: Map[ModelId, Set[(ModelId, ClusterId, CPath, DoubleColumn)]] = {
              schema.columnRefs.flatMap {
                case ColumnRef(path @ CPath(TableModule.paths.Value, CPathField(modelName), CPathField(clusterName), rest @ _*), ctype) => 
                  Schema.mkType((path, ctype) :: Nil) flatMap { case jType =>
                    schema.columns(jType) collectFirst { case (col: DoubleColumn) => col }
                  } map { col =>
                    (modelName, clusterName, CPath((TableModule.paths.Value +: rest): _*), col)
                  }

                case _ => None
              } groupBy { _._1 }
            }

            val modelsByCluster = modelTuples map { case (modelId, models) => (modelId, models.groupBy(_._2)) }

            { (i: Int) =>
              modelsByCluster.map { case (modelId, clusters) => 
                val modelClusters: Array[ModelCluster] = clusters.map { case (clusterId, colInfo) =>
                  val featureValues = colInfo.collect { case (_, _, cpath, col) if col.isDefinedAt(i) => cpath -> col(i) }.toMap
                  ModelCluster(clusterId, featureValues)
                }.toArray
                
                Model(modelId, modelClusters)
              }.toSet
            }
          }

          range.toList map { i => ModelSet(rowIdentities(i), rowModels(i)) }
        }
      }

      protected def morph1Apply(models: Models): Morph1Apply = new Morph1Apply {
        def scanner(modelSet: ModelSet): CScanner = new CScanner {
          type A = Unit
          def init: A = ()

          def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
            def included(model: Model): Map[ColumnRef, Column] = {
              val featurePaths = (model.clusters).flatMap { _.featureValues.keys }.toSet

              val res = cols filter { case (ColumnRef(cpath, ctype), col) =>
                featurePaths.contains(cpath)
              }

              val resPaths = res map { case (ColumnRef(cpath, _), _) => cpath } toSet

              if (resPaths == featurePaths) res
              else Map.empty[ColumnRef, Column]
            }

            def defined(cols: Map[ColumnRef, Column]): BitSet = {
              val columns = cols map { case (_, col) => col }

              BitSetUtil.filteredRange(range) { i =>
                if (columns.isEmpty) false
                else columns.forall(_ isDefinedAt i)
              }
            }

            def filteredRange(cols: Map[ColumnRef, Column]) = range.filter(defined(cols).apply)

            val result: Set[Map[ColumnRef, Column]] = {

              val modelsResult: Set[Map[ColumnRef, Column]] = modelSet.models map { case model =>
                val includedModel = included(model)
                val definedModel = defined(includedModel)

                val clusterIds: Array[String] = model.clusters map { _.name }
                val clusterCenters: Array[Array[Double]] = (model.clusters).map {
                  _.featureValues.toArray.sortBy { case (path, _) => path }.map { case (_, col) => col }.toArray
                }

                val featureColumns0 = includedModel.collect {
                  case (ref, col: DoubleColumn) => (ref, col)
                }.toArray sortBy { case (ColumnRef(path, _), _) => path }
                val featureColumns = featureColumns0 map { case (_, col) => col }

                val numFeatures = featureColumns.size

                // TODO: Make faster with arrays and fast isDefined checking.
                val filtered = filteredRange(includedModel)
                val resultArray = filtered.foldLeft(new Array[String](range.end)) { case (arr, row) =>
                  val feature = new Array[Double](numFeatures)
                  var i = 0
                  while (i < feature.length) {
                    feature(i) = featureColumns(i)(row)
                    i += 1
                  }

                  var minDistSq = Double.PositiveInfinity
                  var minCluster = -1
                  i = 0
                  while (i < clusterCenters.length) {
                    // TODO: Don't box for fancy operators...

                    val diff = (feature - clusterCenters(i))
                    val distSq = diff dot diff
                    if (distSq < minDistSq) {
                      minDistSq = distSq
                      minCluster = i
                    }
                    i += 1
                  }

                  arr(row) = clusterIds(minCluster)
                  arr
                }

                Map(ColumnRef(CPath(TableModule.paths.Value, CPathField(model.name)), CString) -> ArrayStrColumn(definedModel, resultArray))
              }
              
              val identitiesResult: Map[ColumnRef, Column] = {
                val modelIds = modelSet.identity collect { case id if id.isDefined => id.get } toArray
                val modelCols: Map[ColumnRef, Column] = modelIds.zipWithIndex map { case (id, idx) => (ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), CLong), Column.const(id)) } toMap

                val featureCols = cols collect { 
                  case (ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), ctype), col) => 
                    val path = Seq(TableModule.paths.Key, CPathIndex(idx + modelIds.size))
                    (ColumnRef(CPath(path: _*), ctype), col)
                  case c @ (ColumnRef(CPath(TableModule.paths.Key), _), _) => c
                }

                modelCols ++ featureCols
              }

              modelsResult ++ Set(identitiesResult)
            }

            implicit val semigroup = Column.unionRightSemigroup
            val monoidCols = implicitly[Monoid[Map[ColumnRef, Column]]]

            val reduced: Map[ColumnRef, Column] = result.toSet.suml(monoidCols)

            ((), reduced)
          }
        }

        def apply(table: Table, ctx: EvaluationContext): M[Table] = {
          val scanners: Seq[TransSpec1] = models map { model => WrapArray(Scan(TransSpec1.Id, scanner(model))) }
          val spec: TransSpec1 = scanners reduceOption { (s1, s2) => InnerArrayConcat(s1, s2) } getOrElse TransSpec1.Id

          val forcedTable = table.transform(spec).force
          val tables0 = (0 until scanners.size) map { i => forcedTable.map(_.transform(DerefArrayStatic(TransSpec1.Id, CPathIndex(i)))) }
          val tables: M[Seq[Table]] = (tables0.toList).sequence

          tables.map(_.reduceOption { _ concat _ } getOrElse Table.empty)
        }
      }
    }
  }
}
