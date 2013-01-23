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


trait CoreSetClustering {
  type WeightedPoint = (Array[Double], Long)  //(point, weight)
  type CoreSet = Array[WeightedPoint]

  def epsilon: Double

  def cluster(points: Array[Array[Double]], k: Int): Array[Array[Double]]

  case class CoreSetTree(tree: List[(Int, CoreSet)], k: Int) {
    private def insertCoreSet(coreset: CoreSet, level: Int): CoreSetTree = {
      val (prefix, suffix) = tree partition { case (idx, _) => idx < level }

      def rec(tree0: List[(Int, CoreSet)], coreset0: CoreSet, level0: Int): List[(Int, CoreSet)] = {
        tree0 match {
          case (`level0`, coreset1) :: tail => 
            rec(tail, mergeCoreSets(coreset0, corset1, level0), level0 + 1)
          case _ =>
            (level0, coreset0) :: tree0
        }
      }

      CoreSetTree(prefix ++ rec(suffix, coreset, level))
    }

    def ++(coreSetTree: CoreSetTree): CoreSetTree = {
      coreSetTree.tree.foldLeft(tree) { case (acc, (coreset, level)) =>
        acc.insertCoreSet(coreset, level)
      }
    }
  }

  object CoreSetTree {
    def zero(k: Int): CoreSetTree = CoreSetTree(Nil, k)

    def apply(coreSet: CoreSet, k: Int): CoreSetTree = CoreSetTree((0, coreSet) :: Nil, k)

    def fromPoints(points: Array[Array[Double]], k: Int): CoreSetTree = {
      val clustering = cluster(points, k)
      makeCoreSet(points, clustering)
    }
  }

  private def makeCoreSet(points: Array[Array[Double]], clustering: Array[Array[Double]]): CoreSetTree = {
    val (distance, assignments): (Array[Double], Array[Int]) = points map { point =>
      (0 until clustering.length).foldLeft((Double.Infinity, -1)) { case ((minDist, minIdx), idx) =>
        val dist = distSq(point, clustering(idx))
        if (dist < minDist) {
          (dist, idx)
        } else {
          (minDist, minIdx)
        }
      }
    } unzip

    val cost = (distance map (math.sqrt(_))).qsum // K-means cost function.
    val c = 4
    val n = points.length

    val radiusGLB = cost / (c * n)
    val maxResolution = math.ceil(2 * math.log(c * points.length))

    def grid(center: Array[Double]): Array[Double] => GridPoint = {
      val logRadiusGLB = math.log(radiusGLB)
      val log2 = math.log(2d)
      val sideLengths: Array[Double] = (0 to maxResolution).map({ j =>
        epsilon * radiusGLB * math.pow(2d, j) / (10 * c * center.length)
      })(collection.breakOut)

      { (point: Array[Double]) =>
        val minx = distMin(point, center)
        val j = math.max(0, math.ceil((math.log(minx) - logRadiusGLB) / log2).toInt)

        require(j < sideLengths.length, "Point found outside of grid. What to do...")

        val sideLength = sideLengths(j)
        val scaledPoint = (point - center) :/ sideLength
        var i = 0
        while (i < scaledPoint.length) {
          scaledPoint(i) = center(i) + math.floor(scaledPoint(i)) * sideLength + (sideLength / 2)
          i += 1
        }
        new GridPoint(scaledPoint)
      }
    }

    val grids = clustering map grid
    
    val corset = (0 until points.length).foldLeft(Map.empty[GridPoint, Long]) { (weights, idx) =>
      val point = points(i)
      val assignment = assignments(i)
      val gridPoint = grids(assignment)(point)
      weights + (gridPoint -> (weights.getOrElse(gridPoint, 0L) + 1L))
    } map { case (gridPoint, weight) =>
      (gridPoint.point, weight)
    }
   
    CoreSetTree(coreset.toArray)
  }

  def distSq(x: Array[Double], y: Array[Double]): Double = {
    var i = 0
    var acc = 0d
    while (i < x.length && i < y.length) {
      val delta = x(i) - y(i)
      acc += delta * delta
      i += 1
    }
    acc
  }

  def dist(x: Array[Double], y: Array[Double]): Double = math.sqrt(distSq(x, y))

  def distMin(x: Array[Double], y: Array[Double]): Double = {
    var minx = Double.Infinity
    var i = 0
    while (i < x.length && i < y.length) {
      val dx = math.abs(x(i) - y(i))
      if (dx < minx) {
        minx = dx
      }
      i += 1
    }
    minx
  }

  final class GridPoint(point: Array[Double]) {
    def hashDouble(x: Double): Int = {
      val l = java.lang.Double.doubleToLongBits(x)
      l.toInt * 23 + (l >>> 32).toInt
    }

    def hashCode: Int = {
      val hash = point.length * 17
      var i = 0
      while (i < point.length) {
        hash += point(i) * 23
        i += 1
      }
      hash
    }

    def equals(that: Any) = that match {
      case GridPoint(thatPoint) => this === that
      case _ => false
    }
  }
}

trait Clustering[M[+_]] extends GenOpcode[M] {
  import Clustering._

  override def _libMorphism2 = super._libMorphism2 ++ Set(Clustering)

  object KMeansClustering extends Morphism2(Stats3Namespeca, "kmeans") {
    val tpe = BinaryOperationType(JType.JUniverseT, JNumberT, JObjectUnfixedT)

    lazy val alignment = MorphismAlignment.Custom(alignCustom _)

    type KS = List[Int]


    def reducerKS: CReducer[KS] = new CReducer[KS] {
      def reduce(schema: CSchema, range: Range): KS = {
        sys.error("todo")
      }
    }


    implicit def resultMonoid = new Monoid[CoreSetTree] {
      def zero = CoreSetTree.zero
      def append(c1: CoreSetTree, c2: => CoreSetTree) = {
        c1 ++ c2
      }
    }

    val epsilon = 0.1

    //we probably want Array[Array[Double]]
    def reducerFeatures: CReducer[CoreSet] = new CReducer[CoreSet]] {
      def reduce(schema: CSchema, range: Range): Vector[CoreSet] = {
        val features = schema.columns(JArrayHomogeneousT(JNumberT))

        val values: Set[Option[Array[Array[Double]]]] = features map {
          case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
            val mapped = range.toArray filter { r => c.isDefinedAt(r) } map { i => 1.0 +: c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
            Some(mapped)
          case other => 
            logger.warn("Features were not correctly put into a homogeneous array of doubles; returning empty.")
            None
        }

        val arrays0: Option[Array[Array[Double]]] = {
          if (values.isEmpty) None
          else values.suml(resultMonoid)
        }

        val arrays: Array[Array[Double]] = arrays0 getOrElse Array.empty[Features]

        def clustering(arrays: Array[Array[Double]): Array[Array[Double]] = sys.error("todo")

      }
    }

    def extract(res: Result, jtype: JType): Table = {
      val cpaths = Schema.cpath(jtype)

      val tree = CPath.makeTree(cpaths, Range(1, res.length).toSeq :+ 0)

      val spec = TransSpec.concatChildren(tree)

      val theta = Table.constArray(Set(CArray[Double](res)))

      val result = theta.transform(spec)

      val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
      val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

      valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
    }


    def morph1Apply(ks: List[Int]): Morph1Apply = new Morph1Apply {
      def apply(table: Table, ctx: EvaluationContext): M[Table] = {
        val schemas: M[Seq[JType]] = table.schemas map { _.toSeq }
        
        val specs: M[Seq[TransSpec1]] = schemas map {
          _ map { jtype => trans.Typed(TransSpec1.Id, jtype) }
        }

        val tables: M[Seq[Table]] = specs map { ts => ts map { table.transform } }

        M[Seq[StreamT[M, Array[Feature]]]]
        val tables: StreamT[M, Table] = StreamT.wrapEffect {
          specs map { ts =>
            StreamT.fromStream((ts map { table.transform }).toStream)
          }
        }

        val tablesWithType: M[Seq[(Table, JType)]] = for {
          tbls <- tables
          jtypes <- schemas
        } yield {
          tbls zip jtypes
        }

        val sliceSize = 1000
        val features: StreamT[M, Array[Features]] = tables flatMap { tbl =>
          val features = tbl.canonicalize(sliceSize).toArray[Double].normalize.reduce(reducerFeatures)
          StreamT(features map (StreamT.Yield(_, StreamT.empty[M, Array[Feature]])))
        }

        val reducedTables: M[Seq[Table]] = tablesWithType flatMap { 
          _.map { case (table, jtype) => tableReducer(table, jtype) }.toStream.sequence map(_.toSeq)
        }




        sys.error("todo")
      }
    }

    def alignCustom(t1: Table. t2: Table): M[(Table, Morph1Apply)] = {
      t2.transform(spec).reduce(reducerKS) map { ks => 
        t1.transform(liftToValues(TransSpec1.Id), morph1Apply(ks))
      }
    }
  }
}
