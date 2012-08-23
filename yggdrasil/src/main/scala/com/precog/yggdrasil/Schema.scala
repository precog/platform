package com.precog.yggdrasil

import scala.collection.immutable.BitSet

import com.precog.common.json._

import com.precog.bytecode._

object Schema {
  def ctypes(primitive : JPrimitiveType): Set[CType] = primitive match {
    case JArrayHomogeneousT(elemType) => ctypes(elemType) collect {
      case cType: CValueType[_] => CArrayType(cType)
    }
    case JNumberT => Set(CLong, CDouble, CNum)
    case JTextT => Set(CString)
    case JBooleanT => Set(CBoolean)
    case JNullT => Set(CNull)
  }

  private def fromCValueType(t: CValueType[_]): Option[JPrimitiveType] = t match {
    case CBoolean => Some(JBooleanT)
    case CString => Some(JTextT)
    case CLong | CDouble | CNum => Some(JNumberT)
    case CArrayType(elemType) => fromCValueType(elemType) map (JArrayHomogeneousT(_))
    case CDate => None
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

    val indices = ctpes.foldLeft(BitSet()) {
      case (acc, (CPath(CPathIndex(i), _*), _)) => acc+i
      case (acc, _) => acc
    }

    val elements = indices.flatMap { i =>
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

    case (JObjectUnfixedT, (CPath.Identity, CEmptyObject)) => true
    case (JObjectUnfixedT, (CPath(CPathField(_), _*), _)) => true
    case (JObjectFixedT(fields), (CPath.Identity, CEmptyObject)) if fields.isEmpty => true
    case (JObjectFixedT(fields), (CPath(CPathField(head), tail @ _*), ctpe)) =>
      fields.get(head).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)

    case (JArrayUnfixedT, (CPath.Identity, CEmptyArray)) => true
    case (JArrayUnfixedT, (CPath(CPathArray, _*), CArrayType(_))) => true
    case (JArrayUnfixedT, (CPath(CPathIndex(_), _*), _)) => true
    case (JArrayFixedT(elements), (CPath.Identity, CEmptyArray)) if elements.isEmpty => true
    case (JArrayFixedT(elements), (CPath(CPathIndex(i), tail @ _*), ctpe)) =>
      elements.get(i).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)
    case (JArrayHomogeneousT(jElemType), (CPath(CPathArray, _*), CArrayType(cElemType))) =>
      fromCValueType(cElemType) == jElemType

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

    case JTextT => ctpes.exists {
      case (CPath.Identity, CString) => true
      case _ => false
    }

    case JBooleanT => ctpes.contains(CPath.Identity, CBoolean)

    case JNullT => ctpes.contains(CPath.Identity, CNull)

    case JObjectUnfixedT if ctpes.contains(CPath.Identity, CEmptyObject) => true
    case JObjectUnfixedT => ctpes.exists {
      case (CPath(CPathField(_), _*), _) => true
      case _ => false
    }
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
