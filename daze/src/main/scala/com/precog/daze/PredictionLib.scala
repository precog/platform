package com.precog
package daze

import util.{BitSet, BitSetUtil}

import yggdrasil._
import table._

import bytecode._

import common.json._

trait PredictionLib[M[+_]] extends GenOpcode[M] with Evaluator[M] {
  import trans._

  override def _libMorphism2 = super._libMorphism2 ++ Set(LinearPrediction)

  val Stats3Namespace = Vector("std", "stats")

  object LinearPrediction extends Morphism2(Stats3Namespace, "predictLinear") {
    val tpe = BinaryOperationType(JObjectUnfixedT, JType.JUniverseT, JObjectUnfixedT)

    override val multivariate = true  // `true` lets it use Coerce To Double
    
    // need to do a cross since LHS could have Set({Model1:.., Model2:..}, {Model1:..})
    // though results ambiguous since there are two `Model1`
    lazy val alignment = MorphismAlignment.Cross

    def scanner: CScanner = new CScanner {
      type A = Unit
      def init: A = Unit

      def dropPrefixHelper(prefix: CPath, cols: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
        val colsWithPrefix = cols filter { case (ColumnRef(path, _), _) => path.hasPrefix(prefix) }
        colsWithPrefix map { case (ColumnRef(path, ctpe), col) =>
          (ColumnRef(path.dropPrefix(prefix) getOrElse path, ctpe), col) //todo what should getOrElse do?
        }
      }

      def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
        val modelCols = cols filter { case (ColumnRef(path, _), _) => path.hasPrefix(CPath(CPathIndex(0))) }
        val inputCols = cols filter { case (ColumnRef(path, _), _) => path.hasPrefix(CPath(CPathIndex(1))) }

        val modelColsDrop = dropPrefixHelper(CPath(CPathIndex(0)), modelCols)
        
        val models = modelColsDrop map { case (ColumnRef(path, _), _) => path.nodes.head } toSet //todo need headOption, or something like that

        val newModelCols = models map { node => 
          val dropped = dropPrefixHelper(CPath(CPathIndex(0), node, CPathIndex(0)), modelCols)
          (node, dropped)
        } toMap
        

        val newInputCols = models map { node =>
          val dropped = dropPrefixHelper(CPath(CPathIndex(1), node), inputCols)
          (node, dropped)
        } toMap

        val jointModels = newModelCols.keySet intersect newInputCols.keySet

        val combinedModels = jointModels map { model => 
          val m = newModelCols(model)
          val i = newInputCols(model)
          (model, (m, i))
        } toMap

        //todo use lazyMapValues
        val result: Map[CPathNode, Seq[(Column, Column)]] = combinedModels mapValues { case (cols1, cols2) =>
          cols1.toSeq flatMap { case (ColumnRef(modelPath, _), modelCol) =>
            cols2.toSeq collect { case (ColumnRef(inputPath, _), inputCol) if (modelPath == inputPath) =>
              (modelCol, inputCol)
            }
          }
        }

        val defined: BitSet = BitSetUtil.filteredRange(range) {
          i => result exists { case (_, cols) =>
            cols forall { case (c1, c2) => c1.isDefinedAt(i) && c2.isDefinedAt(i) }  //todo doesn't handle numeric columns
          }
        }
        
        def constants: Map[CPathNode, Double] = Map(CPathField("Model1") -> 2.2) //todo

        val result2: Map[CPath, Array[Double]] = result map { case (node, cols) =>
          val pathArray = range.foldLeft(new Array[Double](range.end)) { case (acc, i) =>
            val productCols = cols map { 
              case (c1: DoubleColumn, c2: DoubleColumn) => c1(i) * c2(i)  //todo where does handling of undefineds occur?
              case _ => sys.error("todo")
            }

            acc(i) = productCols.sum + constants(node)
            acc
          }

          (CPath(node), pathArray)
        }

        val result3 = result2 map { case (cpath, values) => 
          (ColumnRef(cpath, CDouble), ArrayDoubleColumn(defined, values))
        }

        (Unit, result3)
      }
    }

    def apply(table: Table, ctx: EvaluationContext) = 
      M.point(table.transform(buildConstantWrapSpec(Scan(TransSpec1.Id, scanner))))
  }
}
