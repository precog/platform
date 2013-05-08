package com.precog
package daze

import yggdrasil._
import yggdrasil.table._

import common._
import com.precog.util.BitSetUtil

import bytecode._
import TableModule._

import Jama._
import Jama.Matrix._

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.std.set._
import scalaz.syntax.traverse._

trait LinearRegressionLibModule[M[+_]] 
    extends ColumnarTableLibModule[M]
    with ReductionLibModule[M]
    with EvaluatorMethodsModule[M]
    with PredictionLibModule[M] {

  trait LinearRegressionLib 
      extends ColumnarTableLib
      with RegressionSupport
      with PredictionSupport
      with ReductionLib
      with EvaluatorMethods {

    import trans._

    override def _libMorphism2 = super._libMorphism2 ++ Set(MultiLinearRegression, LinearPrediction)

    object MultiLinearRegression extends Morphism2(Stats2Namespace, "linearRegression") {
      val tpe = BinaryOperationType(JNumberT, JType.JUniverseT, JObjectUnfixedT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type Beta = Array[Double]

      // `beta`: current weighted value for coefficients
      // `count`: number of rows seen 
      // `removed`: indices of columns removed due to their dependence on other columns
      case class CoeffAcc(beta: Beta, count: Long, removed: Set[Int])

      // `rss`: Residual Sum of Squares
      // `tss`: Total Sum of Squares
      // `product`: product of X and X^T
      case class StdErrorAcc(rss: Double, tss: Double, product: Matrix)

      /**
       * http://adrem.ua.ac.be/sites/adrem.ua.ac.be/files/StreamFitter.pdf
       *
       * First slice size is made consistent. Then the slices are fed to the reducer, one by one.
       * The reducer calculates the Ordinary Least Squares regression for each Slice. 
       * The results of each of these regressions are then combined incrementally using `monoid`. 
       * `alpha` (a value between 0 and 1) is the paramater which determines the weighting of the
       * data in the stream. A value of 0.5 means that current values and past values
       * are equally weighted. The paper above outlines how `alpha` relates to the half-life
       * of the current window (i.e. the current Slice). In the future, we could let half-life,
       * or something related, be an optional parameter in the regression model.
       */


      /**
       * The following code will necessary when we have online models and users want to weight
       * the most recent data with a higher (or lower) weight than the data already seen.
       * But since we don't have this capability yet, all data is weighted equally.

      val alpha = 0.5

      implicit def monoid = new Monoid[CoeffAcc] {
        def zero = Array.empty[Double]
        def append(r1: CoeffAcc, r2: => CoeffAcc) = {
          lazy val newr1 = r1 map { _ * alpha }
          lazy val newr2 = r2 map { _ * (1.0 - alpha) }

          if (r1.isEmpty) r2
          else if (r2.isEmpty) r1
          else arraySum(newr1, newr2)
        }
      }
      */

      implicit def coeffMonoid = new Monoid[CoeffAcc] {
        def zero = CoeffAcc(Array.empty[Double], 0, Set.empty[Int])
        def append(r1: CoeffAcc, r2: => CoeffAcc) = {
          if (r1.beta isEmpty) r2
          else if (r2.beta isEmpty) r1
          //columns are only `removed` if they are not present in *all* slices seen so far
          //this semantic ensures that stdErr computing knows which columns to consider
          else CoeffAcc(arraySum(r1.beta, r2.beta), r1.count + r2.count, r1.removed & r2.removed)
        }
      }

      implicit def stdErrorMonoid = new Monoid[StdErrorAcc] {
        def zero = StdErrorAcc(0, 0, new Matrix(Array(Array.empty[Double])))

        def append(t1: StdErrorAcc, t2: => StdErrorAcc) = {
          def isEmpty(matrix: Matrix) = {
            // there has to be a better way...
            matrix.getArray.length == 1 && matrix.getArray.head.length == 0
          }

          val matrixSum = {
            if (isEmpty(t1.product)) {
              t2.product
            } else if (isEmpty(t2.product)) {
              t1.product
            } else { 
              assert(
                t1.product.getColumnDimension == t2.product.getColumnDimension &&
                t1.product.getRowDimension == t2.product.getRowDimension) 

              t1.product plus t2.product
            }
          }

          StdErrorAcc(t1.rss + t2.rss, t1.tss + t2.tss, matrixSum)
        }
      }

      implicit def betaMonoid = new Monoid[Option[Array[Beta]]] {
        def zero = None
        def append(t1: Option[Array[Beta]], t2: => Option[Array[Beta]]) = {
          t1 match {
            case None => t2
            case Some(c1) => t2 match {
              case None => Some(c1)
              case Some(c2) => Some(c1 ++ c2)
            }
          }
        }
      }

      // inserts the Double value `0` at all `indices` in `values`
      def insertZeroAt(values: Array[Double], indices0: Array[Int]): Array[Double] = {
        val vlength = values.length
        val indices = indices0 filter { i =>
          (i >= 0) && (i < vlength + indices0.length)
        }

        if (indices.isEmpty) {
          values
        } else {
          val zero = 0D
          val length = vlength + indices.length
          val bitset = BitSetUtil.create(indices)
          val acc = new Array[Double](length)

          var i = 0
          var j = 0
          while (i < length) {
            val idx = {
              if (bitset(i)) { j += 1; zero }
              else values(i - j)
            }
            acc(i) = idx
            i += 1
          }
          acc
        }
      }

      // removes `indices` from `values`
      def removeAt(values: Array[Double], indices0: Array[Int]): Array[Double] = {
        val vlength = values.length
        val indices = indices0 filter { i =>
          (i >= 0) && (i < vlength)
        }

        if (indices.isEmpty) {
          values
        } else {
          val length = vlength - indices.length
          val bitset = BitSetUtil.create(indices)
          val acc = new Array[Double](length)

          val kept = (0 until values.length) filterNot { bitset(_) }

          var i = 0
          while (i < length) {
            acc(i) = values(kept(i))
            i += 1
          }
          acc
        }
      }

      def makeArrays(features: Set[Column], range: Range) = {
        val values = features map {
          case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
            val mapped = range.toArray filter { r => c.isDefinedAt(r) } map { i => c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
            Some(mapped)
          case other => 
            logger.warn("Features were not correctly put into a homogeneous array of doubles; returning empty.")
            None
        }

        val arrays = {
          if (values.isEmpty) None
          else values.suml(betaMonoid)
        }

        val xs = arrays collect { case mx if !mx.isEmpty =>
          mx collect { case arr if !arr.isEmpty => 
            1.0 +: (java.util.Arrays.copyOf(arr, arr.length - 1))
          }
        }
        val y0 = arrays collect { case mx if !mx.isEmpty =>
          mx collect { case arr if !arr.isEmpty => arr.last }
        }

        (xs map { _.toArray }, y0 map { _.toArray })
      }

      def coefficientReducer: Reducer[CoeffAcc] = new Reducer[CoeffAcc] {
        def reduce(schema: CSchema, range: Range): CoeffAcc = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          val count = {
            var countAcc = 0L
            RangeUtil.loop(range) { i =>
              if (Column.isDefinedAt(features.toArray, i)) countAcc += 1L
            }
            countAcc
          }

          val (xs, y0) = makeArrays(features, range)

          val matrixX0 = xs map { arr => new Matrix(arr) }

          // FIXME ultimately we do not want to throw an IllegalArgumentException here
          // once the framework is in place, we will return the empty set and issue a warning to the user
          val matrixX1 = matrixX0 map { mx =>
            if (mx.getRowDimension < mx.getColumnDimension) {
              throw new IllegalArgumentException("Matrix is rank deficient. Not enough rows to determine model.")
            } else {
              mx
            }
          }

          def removeColumn(matrix: Matrix, colDim: Int, idx: Int): Matrix = {
            val ids = 0 until colDim
            val columnIndices = ids.take(idx) ++ ids.drop(idx + 1)
            val rowDim = matrix.getRowDimension

            matrix.getMatrix(0, rowDim - 1, columnIndices.toArray)
          }

          // returns a matrix with independent columns and
          // the set of column indices removed from original matrix
          def findDependents(matrix0: Matrix): (Matrix, Set[Int]) = {
            val matrixRank0 = matrix0.rank
            val colDim0 = matrix0.getColumnDimension

            def inner(matrix: Matrix, idx: Int, colDim: Int, removed: Set[Int]): (Matrix, Set[Int]) = {
              if (idx <= colDim) {
                if (matrixRank0 == colDim) {
                  (matrix, removed)
                } else if (matrixRank0 < colDim) {
                  val retained = removeColumn(matrix, colDim, idx)
                  val rank = retained.rank

                  if (rank == matrixRank0)
                    inner(retained, idx, colDim - 1, removed + (idx + removed.size))
                  else if (rank < matrixRank0)
                    inner(matrix, idx + 1, colDim, removed)
                  else
                    sys.error("Rank cannot increase when a column is removed.")
                } else {
                  sys.error("Matrix cannot have rank larger than number of columns.")
                }
              } else {
                sys.error("Failed to find dependent columns. Should never reach this case.")
              }
            }

            inner(matrix0, 1, colDim0, Set.empty[Int])
          }

          val cleaned: Option[(Matrix, Set[Int])] = matrixX1 map { findDependents }

          val matrixY = y0 map { case arr => new Matrix(Array(arr)) }

          val matrixX = for {
            (x, _) <- cleaned
            y <- matrixY
          } yield {
            x.solve(y.transpose)
          }

          val res = matrixX map { _.getArray flatten } getOrElse Array.empty[Double]

          // We weight the results to handle slices of different sizes.
          // Even though we canonicalize the slices to bound their size,
          // but their sizes still may vary
          val weightedRes0 = res map { _ * count }

          val removed = cleaned map { _._2 } getOrElse Set.empty[Int]
          val weightedRes = insertZeroAt(weightedRes0, removed.toArray) 

          CoeffAcc(weightedRes, count, removed)
        }
      }

      def stdErrorReducer(acc: CoeffAcc): Reducer[StdErrorAcc] = new Reducer[StdErrorAcc] {
        def reduce(schema: CSchema, range: Range): StdErrorAcc = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          val (xs, y0) = makeArrays(features, range)

          def removeColumns(matrix: Matrix, rowDim: Int, colDim: Int, idx: Set[Int]): Matrix = {
            val columnIndices = (0 until colDim).toArray filterNot { idx.contains(_) }
            matrix.getMatrix(0, rowDim - 1, columnIndices)
          }

          val matrixX0 = xs map { case arr => new Matrix(arr) }
          val matrixX = matrixX0 map { mx =>
            removeColumns(mx, mx.getRowDimension, mx.getColumnDimension, acc.removed)
          }

          val matrixProduct = matrixX map { matrix => matrix.transpose() times matrix } getOrElse { stdErrorMonoid.zero.product }

          val actualY0 = y0 map { arr => (new Matrix(Array(arr))).transpose() }

          val retainedBeta = removeAt(acc.beta, acc.removed.toArray)
          val weightedBeta = retainedBeta map { _ / acc.count }

          val predictedY0 = matrixX map { _.times((new Matrix(Array(weightedBeta))).transpose()) }

          val rssOpt = for {
            actualY <- actualY0
            predictedY <- predictedY0
          } yield {
            val difference = actualY.minus(predictedY)
            val prod = difference.transpose() times difference

            assert(prod.getRowDimension == 1 && prod.getColumnDimension == 1)
            prod.getArray.head.head
          }

          val rss = rssOpt getOrElse { stdErrorMonoid.zero.rss }

          val yMean = y0.flatten.sum / y0.flatten.size

          val tss = y0.flatten map { y => math.pow(y - yMean, 2d) } sum

          StdErrorAcc(rss, tss, matrixProduct)
        }
      }

      def extract(coeffs: CoeffAcc, errors: StdErrorAcc, jtype: JType): Table = {
        val cpaths = Schema.cpath(jtype)

        val tree = CPath.makeTree(cpaths, Range(1, coeffs.beta.length).toSeq :+ 0)

        val spec = TransSpec.concatChildren(tree)

        val weightedBeta = coeffs.beta map { _ / coeffs.count }

        val colDim = errors.product.getColumnDimension

        val degOfFreedom = coeffs.count - colDim
        val varianceEst = errors.rss / degOfFreedom
        
        val inverse = errors.product.inverse()
        val varianceCovariance = inverse.times(varianceEst)

        val stdErrors0 = (0 until colDim) map { case i => math.sqrt(varianceCovariance.get(i, i)) }
        val stdErrors = insertZeroAt(stdErrors0.toArray, coeffs.removed.toArray)

        assert(weightedBeta.length == stdErrors.length)

        val resultCoeffs: Array[CValue] = weightedBeta.map(CNum(_)).toArray
        val resultErrors: Array[CValue] = stdErrors.map(CNum(_)).toArray

        val arr = resultCoeffs.zip(resultErrors) map { case (beta, error) =>
          RObject(Map("estimate" -> beta, "standardError" -> error))
        }

        val theta = Table.fromRValues(Stream(RArray(arr.toList)))

        val thetaInSchema = theta.transform(spec)

        val varCovarArr = varianceCovariance.getArray
        val varCovarRv = RArray(varCovarArr map { arr => RArray(arr.map(CNum(_)): _*) }: _*)
        val varCovarTable0 = Table.fromRValues(Stream(varCovarRv))

        val varCovarTable = varCovarTable0.transform(trans.WrapObject(Leaf(Source), "varianceCovarianceMatrix"))

        val coeffsTable = thetaInSchema.transform(trans.WrapObject(Leaf(Source), "coefficients"))

        val stdErrResult = RObject(Map("estimate" -> CNum(math.sqrt(varianceEst)), "degreesOfFreedom" -> CNum(degOfFreedom)))
        val residualStdError = Table.fromRValues(Stream(stdErrResult))
        val stdErrorTable = residualStdError.transform(trans.WrapObject(Leaf(Source), "residualStandardError"))

        val rSquared = {
          if (errors.tss == 0) 1
          else 1 - (errors.rss / errors.tss)
        }
        val rSquaredTable0 = Table.fromRValues(Stream(CNum(rSquared)))
        val rSquaredTable = rSquaredTable0.transform(trans.WrapObject(Leaf(Source), "RSquared"))

        val result2 = coeffsTable.cross(rSquaredTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        val result1 = result2.cross(varCovarTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        val result = result1.cross(stdErrorTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))

        val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
        val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

        valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
      }

      private val morph1 = new Morph1Apply {
        def apply(table0: Table, ctx: EvaluationContext) = {
          val ySpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(0))
          val xsSpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(1))

          val ySpec = trans.Map1(ySpec0, cf.util.CoerceToDouble)
          val xsSpec = trans.DeepMap1(xsSpec0, cf.util.CoerceToDouble)

          // `arraySpec` generates the schema in which the Coefficients will be returned
          val arraySpec = InnerArrayConcat(trans.WrapArray(xsSpec), trans.WrapArray(ySpec))
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          val table = table0.transform(valueSpec).transform(arraySpec)

          val schemas: M[Seq[JType]] = table.schemas map { _.toSeq }
          
          val specs: M[Seq[TransSpec1]] = schemas map {
            _ map { jtype => trans.Typed(TransSpec1.Id, jtype) }
          }

          val tables: M[Seq[Table]] = specs map { _ map { table.transform } }

          val tablesWithType: M[Seq[(Table, JType)]] = for {
            tbls <- tables
            jtypes <- schemas
          } yield {
            tbls zip jtypes
          }

          // important note: regression will explode if there are more than `sliceSize` columns due to rank-deficient matrix
          // this could be remedied in the future by smarter choice of `sliceSize`
          // though do we really want to allow people to run regression on >`sliceSize` columns?
          val sliceSize = 10000
          val tableReducer: (Table, JType) => M[Table] = {
            (table, jtype) => {
              val arrayTable = table.canonicalize(sliceSize, Some(sliceSize * 2)).toArray[Double]

              val coeffs0 = arrayTable.reduce(coefficientReducer)
              val errors0 = coeffs0 flatMap { acc => arrayTable.reduce(stdErrorReducer(acc)) }

              for {
                coeffs <- coeffs0
                errors <- errors0
              } yield {
                extract(coeffs, errors, jtype)
              }
            }
          }

          val reducedTables: M[Seq[Table]] = tablesWithType flatMap { 
            _.map { case (table, jtype) => tableReducer(table, jtype) }.toStream.sequence map(_.toSeq)
          }

          val objectTables: M[Seq[Table]] = reducedTables map { 
            _.zipWithIndex map { case (tbl, idx) =>
              val modelId = "model" + (idx + 1)
              tbl.transform(liftToValues(trans.WrapObject(TransSpec1.Id, modelId)))
            }
          }

          val spec = OuterObjectConcat(
            DerefObjectStatic(Leaf(SourceLeft), paths.Value),
            DerefObjectStatic(Leaf(SourceRight), paths.Value))

          objectTables map { _.reduceOption {
            (tl, tr) => tl.cross(tr)(buildConstantWrapSpec(spec))
          } getOrElse Table.empty }
        }
      }
    }

    object LinearPrediction extends Morphism2(Stats2Namespace, "predictLinear") with LinearPredictionBase {
      val tpe = BinaryOperationType(JType.JUniverseT, JObjectUnfixedT, JObjectUnfixedT)

      override val idPolicy = IdentityPolicy.Retain.Merge

      lazy val alignment = MorphismAlignment.Custom(IdentityPolicy.Retain.Cross, alignCustom _)

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val spec = liftToValues(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble))
        t2.transform(spec).reduce(reducer) map { models =>
          (t1.transform(spec), morph1Apply(models, scala.Predef.identity[Double]))
        }
      }
    }
  }
}
