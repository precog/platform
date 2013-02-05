package com.precog
package daze

import yggdrasil._
import yggdrasil.table._

import common.json._

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

trait LinearRegressionLibModule[M[+_]] extends ColumnarTableLibModule[M] with EvaluatorMethodsModule[M] with PredictionLibModule[M] {
  trait LinearRegressionLib extends ColumnarTableLib with RegressionSupport with PredictionSupport with EvaluatorMethods {
    import trans._

    override def _libMorphism2 = super._libMorphism2 ++ Set(MultiLinearRegression, LinearPrediction)

    object MultiLinearRegression extends Morphism2(Stats2Namespace, "linearRegression") {
      val tpe = BinaryOperationType(JType.JUniverseT, JNumberT, JObjectUnfixedT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type Beta = Array[Double]
      type Result = Beta

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

      val alpha = 0.5

      implicit def monoid = new Monoid[Result] {
        def zero = Array.empty[Double]
        def append(r1: Result, r2: => Result) = {
          lazy val newr1 = r1 map { _ * alpha }
          lazy val newr2 = r2 map { _ * (1.0 - alpha) }

          if (r1.isEmpty) r2
          else if (r2.isEmpty) r1
          else arraySum(newr1, newr2)
        }
      }

      implicit def resultMonoid = new Monoid[Option[Array[Result]]] {
        def zero = None
        def append(t1: Option[Array[Result]], t2: => Option[Array[Result]]) = {
          t1 match {
            case None => t2
            case Some(c1) => t2 match {
              case None => Some(c1)
              case Some(c2) => Some(c1 ++ c2)
            }
          }
        }
      }

      def reducer: Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          val values: Set[Option[Array[Array[Double]]]] = features map {
            case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
              val mapped = range.toArray filter { r => c.isDefinedAt(r) } map { i => 1.0 +: c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
              Some(mapped)
            case other => 
              logger.warn("Features were not correctly put into a homogeneous array of doubles; returning empty.")
              None
          }

          val arrays = {
            if (values.isEmpty) None
            else values.suml(resultMonoid)
          }

          val xs = arrays map { _ map { arr => java.util.Arrays.copyOf(arr, arr.length - 1) } }
          val y0 = arrays map { _ map { _.last } }

          val matrixX = xs map { case arr => new Matrix(arr) }

          // FIXME ultimately we do not want to throw an IllegalArgumentException here
          // once the framework is in place, we will return the empty set and issue a warning to the user
          // this catches the case when the user runs regression on data when rows < (columns + 1)
          val inverseX = try {
            matrixX map { _.inverse() }
          } catch {
            case ex: RuntimeException if ex.getMessage == "Matrix is rank deficient." => 
              throw new IllegalArgumentException("More features than rows found in linear regression. Not enough information to determine model.", ex)
          }

          val matrixY = y0 map { case arr => new Matrix(Array(arr)) }

          val matrixProduct: Option[Matrix] = for {
            inverse <- inverseX
            y <- matrixY
          } yield {
            (inverse).times(y.transpose)
          }

          matrixProduct map { _.getArray flatten } getOrElse Array.empty[Double]
        }
      }

      def extract(res: Result, jtype: JType): Table = {
        val cpaths = Schema.cpath(jtype)

        val tree = CPath.makeTree(cpaths, Range(1, res.length).toSeq :+ 0)

        val spec = TransSpec.concatChildren(tree)
  
        val theta = Table.fromJson(Stream(JArray(res.map(JNum(_)).toList)))

        val result = theta.transform(spec)

        val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
        val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

        valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
      }

      private val morph1 = new Morph1Apply {
        def apply(table0: Table, ctx: EvaluationContext) = {
          val leftSpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(0))
          val rightSpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(1))

          val leftSpec = trans.DeepMap1(leftSpec0, cf.util.CoerceToDouble)
          val rightSpec = trans.Map1(rightSpec0, cf.util.CoerceToDouble)

          val table = table0.transform(InnerArrayConcat(trans.WrapArray(leftSpec), trans.WrapArray(rightSpec)))

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
      
          // important note: regression will explode if there are more than 1000 columns due to rank-deficient matrix
          // this could be remedied in the future by smarter choice of `sliceSize`
          // though do we really want to allow people to run regression on >1000 columns?
          val sliceSize = 1000
          val tableReducer: (Table, JType) => M[Table] =
            (table, jtype) => table.canonicalize(sliceSize).toArray[Double].normalize.reduce(reducer).map(res => extract(res, jtype))

          val reducedTables: M[Seq[Table]] = tablesWithType flatMap { 
            _.map { case (table, jtype) => tableReducer(table, jtype) }.toStream.sequence map(_.toSeq)
          }

          val defaultNumber = new java.util.concurrent.atomic.AtomicInteger(1)

          val objectTables: M[Seq[Table]] = reducedTables map { 
            _ map { tbl =>
              val modelId = "Model" + defaultNumber.getAndIncrement.toString
              tbl.transform(liftToValues(trans.WrapObject(TransSpec1.Id, modelId)))
            }
          }

          val spec = OuterObjectConcat(
            DerefObjectStatic(Leaf(SourceLeft), paths.Value),
            DerefObjectStatic(Leaf(SourceRight), paths.Value))

          objectTables map { _.reduceOption { (tl, tr) => tl.cross(tr)(buildConstantWrapSpec(spec)) } getOrElse Table.empty }
        }
      }
    }

    object LinearPrediction extends Morphism2(Stats2Namespace, "predictLinear") with PredictionBase {
      val tpe = BinaryOperationType(JObjectUnfixedT, JType.JUniverseT, JObjectUnfixedT)

      override val retainIds = true

      lazy val alignment = MorphismAlignment.Custom(alignCustom _)

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val spec = liftToValues(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble))
        t2.transform(spec).reduce(reducer) map { models => (t1.transform(spec), morph1Apply(models, scala.Predef.identity[Double])) }
      }
    }
  }
}
