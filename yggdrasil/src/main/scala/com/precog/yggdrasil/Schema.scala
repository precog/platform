package com.precog.yggdrasil

import table._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import com.precog.common._
import com.precog.bytecode._

object Schema {
  def ctypes(jtype: JType): Set[CType] = jtype match {
    case JArrayFixedT(indices) if indices.isEmpty => Set(CEmptyArray)
    case JObjectFixedT(fields) if fields.isEmpty => Set(CEmptyObject)
    case JArrayFixedT(indices) => indices.values.toSet.flatMap { tpe: JType => ctypes(tpe) }
    case JObjectFixedT(fields) => fields.values.toSet.flatMap { tpe: JType => ctypes(tpe) }
    case JArrayHomogeneousT(elemType) => ctypes(elemType) collect {
      case cType: CValueType[_] => CArrayType(cType)
    }
    case JNumberT => Set(CLong, CDouble, CNum)
    case JTextT => Set(CString)
    case JBooleanT => Set(CBoolean)
    case JNullT => Set(CNull)
    case JDateT => Set(CDate)
    case JPeriodT => Set(CPeriod)
    case _ => Set.empty
  }

  def cpath(jtype: JType): Seq[CPath] = {
    val cpaths = jtype match {
      case JArrayFixedT(indices) => indices flatMap { case (idx, tpe) => CPath(CPathIndex(idx)) combine cpath(tpe) } toSeq
      case JObjectFixedT(fields) => fields flatMap { case (name, tpe) => CPath(CPathField(name)) combine cpath(tpe) } toSeq
      case JArrayHomogeneousT(elemType) => Seq(CPath(CPathArray))
      case JNumberT | JTextT | JBooleanT | JNullT | JDateT => Nil
      case _ => Nil
    }

    cpaths sorted
  }

  def sample(jtype: JType, size: Int): Option[JType] = {
    val paths = flatten(jtype, Nil) groupBy { _._1 } toSeq
    val sampledPaths: Seq[(CPath, CType)] = scala.util.Random.shuffle(paths).take(size) flatMap { _._2 }
    
    mkType(sampledPaths)
  }

  def flatten(jtype: JType, refsOriginal: List[ColumnRef]): List[(CPath, CType)] = {
    def buildPath(nodes: List[CPathNode], refs: List[ColumnRef], jType: JType): List[(CPath, CType)] = jType match {
      case JArrayFixedT(indices) if indices.isEmpty =>
        (CPath(nodes.reverse), CEmptyArray) :: Nil
        
      case JObjectFixedT(fields) if fields.isEmpty =>
        (CPath(nodes.reverse), CEmptyObject) :: Nil
        
      case JArrayFixedT(indices) =>
        indices.toList.flatMap { case (idx, tpe) =>
          val refs0 = refs collect { case ColumnRef(CPath(CPathIndex(`idx`), rest @ _*), ctype) =>
            ColumnRef(CPath(rest: _*), ctype)
          }
          buildPath(CPathIndex(idx) :: nodes, refs0, tpe)
        }

      case JObjectFixedT(fields) => {
        fields.toList.flatMap { case (field, tpe) =>
          val refs0 = refs collect { case ColumnRef(CPath(CPathField(`field`), rest @ _*), ctype) =>
            ColumnRef(CPath(rest: _*), ctype)
          }
          buildPath(CPathField(field) :: nodes, refs0, tpe)
        }
      }

      case JArrayUnfixedT =>
        refs collect { 
          case ColumnRef(p @ CPath(CPathIndex(i), rest @ _*), ctype) =>
            (CPath(nodes.reverse) \ p, ctype)
          case ColumnRef(CPath(), CEmptyArray) =>
            (CPath(nodes.reverse), CEmptyArray)
        }

      case JObjectUnfixedT =>
        refs collect { 
          case ColumnRef(p @ CPath(CPathField(i), rest @ _*), ctype) =>
            (CPath(nodes.reverse) \ p, ctype)
          case ColumnRef(CPath(), CEmptyObject) =>
            (CPath(nodes.reverse), CEmptyObject)
        }

      case JArrayHomogeneousT(tpe) =>
        val refs0 = refs collect { case ColumnRef(CPath(CPathArray, rest @ _*), ctype) =>
          ColumnRef(CPath(rest: _*), ctype)
        }
        buildPath(CPathArray :: nodes, refs0, tpe)

      case JNumberT =>
        val path = CPath(nodes.reverse)
        (path, CLong: CType) :: (path, CDouble) :: (path, CNum) :: Nil

      case JTextT =>
        (CPath(nodes.reverse), CString) :: Nil

      case JBooleanT =>
        (CPath(nodes.reverse), CBoolean) :: Nil

      case JDateT =>
        (CPath(nodes.reverse), CDate) :: Nil

      case JPeriodT =>
        (CPath(nodes.reverse), CPeriod) :: Nil

      case JNullT =>
        (CPath(nodes.reverse), CNull) :: Nil

      case JUnionT(ltpe, rtpe) =>
        buildPath(nodes, refs, ltpe) ++ buildPath(nodes, refs, rtpe)
    }

    buildPath(Nil, refsOriginal, jtype)
  }

  private def fromCValueType(t: CValueType[_]): Option[JType] = t match {
    case CBoolean => Some(JBooleanT)
    case CString => Some(JTextT)
    case CLong | CDouble | CNum => Some(JNumberT)
    case CArrayType(elemType) => fromCValueType(elemType) map (JArrayHomogeneousT(_))
    case CDate => Some(JDateT)
    case CPeriod => Some(JPeriodT)
    case _ => None
  }

  /**
  * replaces all leaves in `jtype` with `leaf`
  */
  def replaceLeaf(jtype: JType)(leaf: JType): JType = {
    def inner(jtype: JType): JType = jtype match {
      case JNumberT | JTextT | JBooleanT | JNullT | JDateT | JPeriodT => leaf
      case JArrayFixedT(elements) => JArrayFixedT(elements.mapValues(inner))
      case JObjectFixedT(fields) => JObjectFixedT(fields.mapValues(inner))
      case JUnionT(left, right) => JUnionT(inner(left), inner(right))
      case JArrayHomogeneousT(tpe) => JArrayHomogeneousT(inner(tpe))
      case arr @ JArrayUnfixedT => arr
      case obj @ JObjectUnfixedT => obj
    }

    inner(jtype)
  }
  
  /**
  * returns a function that, for a given (row: Int), produces a Boolean
  * value is true if the given row subsumes the provided `jtpe`
  */
  def findTypes(jtpe: JType, seenPath: CPath, cols: Map[ColumnRef, Column], size: Int): Int => Boolean = {
    def handleRoot(providedCTypes: Seq[CType], cols: Map[ColumnRef, Column]) = {
      val filteredCols = cols filter { case (ColumnRef(path, ctpe), _) =>
        path == seenPath && providedCTypes.contains(ctpe)
      }
      val bits = filteredCols.values map { 
        _.definedAt(0, size)
      } reduceOption { _ | _ } getOrElse new BitSet

      (row: Int) => bits(row)
    }

    def handleUnfixed(emptyCType: CType, checkNode: CPathNode => Boolean, cols: Map[ColumnRef, Column]) = {
      val objCols = cols filter { case (ColumnRef(path, ctpe), _) =>
        val emptyCrit = path == seenPath && ctpe == emptyCType

        lazy val seenPathLength = seenPath.nodes.length
        lazy val nonemptyCrit = {
          if (seenPathLength + 1 <= path.nodes.length) {
            val pathToCompare = path.nodes.take(seenPathLength)
            pathToCompare == seenPath.nodes && checkNode(path.nodes(seenPathLength))
          } else {
            false
          }
        }

        emptyCrit || nonemptyCrit
      }
      val objBits = objCols.values map { 
        _.definedAt(0, size)
      } reduceOption { _ | _ } getOrElse new BitSet

      (row: Int) => objBits(row)
    }

    def handleEmpty(emptyCType: CType, cols: Map[ColumnRef, Column]) = {
      val emptyCols = cols filter { case (ColumnRef(path, ctpe), _) =>
        path == seenPath && ctpe == emptyCType
      }
      val emptyBits = emptyCols.values map {
        _.definedAt(0, size)
      } reduceOption { _ | _ } getOrElse new BitSet

      (row: Int) => emptyBits(row)
    }

    def combineFixedResults(results: Seq[Int => Boolean]): Int => Boolean = {
      (row: Int) => results.foldLeft(true) { case (bool, fcn) => bool && fcn(row) }
    }

    jtpe match {
      case JNumberT => handleRoot(Seq(CDouble, CLong, CNum), cols)
      case JBooleanT => handleRoot(Seq(CBoolean), cols)
      case JTextT => handleRoot(Seq(CString), cols)
      case JNullT => handleRoot(Seq(CNull), cols)

      case JDateT => handleRoot(Seq(CDate), cols)
      case JPeriodT => handleRoot(Seq(CPeriod), cols)

      case JObjectUnfixedT => handleUnfixed(CEmptyObject, _.isInstanceOf[CPathField], cols)
      case JArrayUnfixedT => handleUnfixed(CEmptyArray, _.isInstanceOf[CPathIndex], cols)

      case JObjectFixedT(fields) =>
        if (fields.isEmpty) {
          handleEmpty(CEmptyObject, cols)
        } else {
          val results: Seq[Int => Boolean] = fields.toSeq map { case (field, tpe) =>
            val seenPath0 = CPath(seenPath.nodes :+ CPathField(field))
            findTypes(tpe, seenPath0, cols, size) 
          }

          combineFixedResults(results)
        }

      case JArrayFixedT(elements) =>
        if (elements.isEmpty) {
          handleEmpty(CEmptyArray, cols)
        } else {
          val results: Seq[Int => Boolean] = elements.toSeq map { case (idx, tpe) =>
            val seenPath0 = CPath(seenPath.nodes :+ CPathIndex(idx))
            findTypes(tpe, seenPath0, cols, size)
          }

          combineFixedResults(results)
        }

      case JUnionT(left, right) =>
        val leftTypes = findTypes(left, seenPath, cols, size)
        val rightTypes = findTypes(right, seenPath, cols, size)

        (row: Int) => leftTypes(row) || rightTypes(row)

      case JArrayHomogeneousT(jtpe) =>
        findTypes(jtpe, CPath(seenPath.nodes :+ CPathArray), cols, size)
    }
  }

  /**
   * Constructs a JType corresponding to the supplied sequence of (CPath, CType) pairs. Returns None if the
   * supplied sequence is empty.
   */
  def mkType(ctpes: Seq[(CPath, CType)]): Option[JType] = {
    
    val primitives = ctpes flatMap {
      case (CPath.Identity, t: CValueType[_]) => fromCValueType(t)
      case (CPath.Identity, CNull) => Some(JNullT)
      case (CPath.Identity, CEmptyArray) => Some(JArrayFixedT(Map()))
      case (CPath.Identity, CEmptyObject) => Some(JObjectFixedT(Map()))
      case _ => None
    }

    val elements = ctpes.collect {
      case (CPath(CPathIndex(i), _*), _) => i
    }.toSet.flatMap {
      (i: Int) =>
      mkType(ctpes.collect {
        case (CPath(CPathIndex(`i`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe)
      }).map(i -> _)
    }
    val array = if (elements.isEmpty) Nil else List(JArrayFixedT(elements.toMap))

    val keys = ctpes.foldLeft(Set.empty[String]) {
      case (acc, (CPath(CPathField(key), _*), _)) => acc+key
      case (acc, _) => acc
    }

    val members = keys.flatMap { key =>
      mkType(ctpes.collect {
        case (CPath(CPathField(`key`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe)
      }).map(key -> _)
    }
    val obj = if (members.isEmpty) Nil else List(JObjectFixedT(members.toMap))

    (primitives ++ array ++ obj).reduceOption(JUnionT)
  }


  /**
   * This is a less-strict version of `includes`. Instead of returning true iff
   * the `(CPath, CType)` is included in the `JType`, it returns `true` if the
   * `(CPath, CType)` pair may be required to satisify some requirement of the
   * `JType`, even if the `(CPath, CType)` may contain more than necessary (eg.
   * in the case of homogeneous arrays when only need a few elements).
   */
  def requiredBy(jtpe: JType, path: CPath, ctpe: CType): Boolean =
    includes(jtpe, path, ctpe) || ((jtpe, path, ctpe) match {
      case (JArrayFixedT(elements), CPath(CPathArray, tail @ _*), CArrayType(elemType)) =>
        elements.values exists (requiredBy(_, CPath(tail: _*), elemType))
      case _ => false
    })

  /**
   * Tests whether the supplied JType includes the supplied CPath and CType.
   *
   * This is strict, so a JArrayFixedT(_) cannot include a CPathArray/CArrayType(_).
   */
  def includes(jtpe: JType, path: CPath, ctpe: CType): Boolean = (jtpe, (path, ctpe)) match {
    case (JNumberT, (CPath.Identity, CLong | CDouble | CNum)) => true

    case (JTextT, (CPath.Identity, CString)) => true

    case (JBooleanT, (CPath.Identity, CBoolean)) => true

    case (JNullT, (CPath.Identity, CNull))=> true

    case (JDateT, (CPath.Identity, CDate))=> true
    case (JPeriodT, (CPath.Identity, CPeriod))=> true

    case (JObjectUnfixedT, (CPath.Identity, CEmptyObject)) => true
    case (JObjectUnfixedT, (CPath(CPathField(_), _*), _)) => true
    case (JObjectFixedT(fields), (CPath.Identity, CEmptyObject)) if fields.isEmpty => true

    case (JObjectFixedT(fields), (CPath(CPathField(head), tail @ _*), ctpe)) => {
      fields.get(head).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)
    }

    case (JArrayUnfixedT, (CPath.Identity, CEmptyArray)) => true
    case (JArrayUnfixedT, (CPath(CPathArray, _*), CArrayType(_))) => true
    case (JArrayUnfixedT, (CPath(CPathIndex(_), _*), _)) => true
    case (JArrayFixedT(elements), (CPath.Identity, CEmptyArray)) if elements.isEmpty => true
    case (JArrayFixedT(elements), (CPath(CPathIndex(i), tail @ _*), ctpe)) =>
      elements.get(i).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)
    case (JArrayHomogeneousT(jElemType), (CPath(CPathArray, _*), CArrayType(cElemType))) =>
      fromCValueType(cElemType) == Some(jElemType)

    // TODO This is a bit contentious, as this situation will need to be dealt
    // with at a higher level if we let parts of a heterogeneous array fall
    // through, posing as a homogeneous array. Especially since, eg, someone
    // should be expecting that if a[1] exists, therefore a[0] exists.
    case (JArrayHomogeneousT(jElemType), (CPath(CPathIndex(i), tail @ _*), ctpe)) =>
      ctypes(jElemType) contains ctpe

    case (JUnionT(ljtpe, rjtpe), (path, ctpe)) => includes(ljtpe, path, ctpe) || includes(rjtpe, path, ctpe)

    case _ => false
  }

  /**
   * Tests whether the supplied sequence contains all the (CPath, CType) pairs that are
   * included by the supplied JType.
   */
  def subsumes(ctpes: Seq[(CPath, CType)], jtpe: JType): Boolean = jtpe match {
    case JNumberT => ctpes.exists {
      case (CPath.Identity, CLong | CDouble | CNum) => true
      case _ => false
    }

    case JTextT => ctpes.contains(CPath.Identity -> CString)

    case JBooleanT => ctpes.contains(CPath.Identity, CBoolean)

    case JNullT => ctpes.contains(CPath.Identity, CNull)

    case JDateT => ctpes.contains(CPath.Identity, CDate)
    case JPeriodT => ctpes.contains(CPath.Identity, CPeriod)

    case JObjectUnfixedT if ctpes.contains(CPath.Identity, CEmptyObject) => true
    case JObjectUnfixedT => ctpes.exists {
      case (CPath(CPathField(_), _*), _) => true
      case _ => false
    }
    case JObjectFixedT(fields) if fields.isEmpty => ctpes.contains(CPath.Identity, CEmptyObject)
    case JObjectFixedT(fields) => {
      val keys = fields.keySet
      keys.forall { key =>
        subsumes(
          ctpes.collect { case (CPath(CPathField(`key`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe) }, 
          fields(key))
      }
    }

    case JArrayUnfixedT if ctpes.contains(CPath.Identity, CEmptyArray) => true
    case JArrayUnfixedT => ctpes.exists {
      case (CPath(CPathArray, _*), _) => true
      case (CPath(CPathIndex(_), _*), _) => true
      case _ => false
    }
    case JArrayFixedT(elements) if elements.isEmpty => ctpes.contains(CPath.Identity, CEmptyArray)
    case JArrayFixedT(elements) => {
      val indices = elements.keySet
      indices.forall { i =>
        subsumes(
          ctpes.collect {
            case (CPath(CPathArray, tail @ _*), CArrayType(elemType)) => (CPath(tail: _*), elemType)
            case (CPath(CPathIndex(`i`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe)
          }, elements(i))
      }
    }
    case JArrayHomogeneousT(jElemType) => ctpes.exists {
      case (CPath(CPathArray, _*), CArrayType(cElemType)) =>
        ctypes(jElemType) contains cElemType
      case _ => false
    }

    case JUnionT(ljtpe, rjtpe) => subsumes(ctpes, ljtpe) || subsumes(ctpes, rjtpe)

    case _ => false
  }
}
