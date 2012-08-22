package com.precog.yggdrasil

import scala.collection.immutable.BitSet

import com.precog.common.json._

import com.precog.bytecode._

object Schema {
  def ctypes(primitive : JPrimitiveType): Set[CType] = primitive match {
    case JNumberT => Set(CLong, CDouble, CNum)
    case JTextT => Set(CString)
    case JBooleanT => Set(CBoolean)
    case JNullT => Set(CNull)
  }

  /**
   * Constructs a JType corresponding to the supplied sequence of (CPath, CType) pairs. Returns None if the
   * supplied sequence is empty.
   */
  def mkType(ctpes: Seq[(CPath, CType)]): Option[JType] = {
    val primitives = ctpes.collect {
      case (CPath.Identity, CLong | CDouble | CNum) => JNumberT
      case (CPath.Identity, CString) => JTextT
      case (CPath.Identity, CBoolean) => JBooleanT
      case (CPath.Identity, CNull) => JNullT
      case (CPath.Identity, CEmptyArray) => JArrayFixedT(Map())
      case (CPath.Identity, CEmptyObject) => JObjectFixedT(Map())
      // case (CPath.Identity, CArrayType(elemType)) => JArrayUnfixedT
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
   * Tests whether the supplied JType includes the supplied CPath and CType.
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
    case (JArrayUnfixedT, (CPath.Identity, CArrayType(_))) => true
    case (JArrayUnfixedT, (CPath(CPathIndex(_), _*), _)) => true
    case (JArrayFixedT(elements), (CPath.Identity, CEmptyArray)) if elements.isEmpty => true
    case (JArrayFixedT(elements), (CPath(CPathIndex(i), tail @ _*), ctpe)) =>
      elements.get(i).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)

    // TODO This isn't really true and we can never know for sure that it is.
    // Commented out for now, but need to investigate if a valid use-case for
    // this may actually happen; ie. someone wants an array.
    //case (JArrayFixedT(elements), (CPath.Identity, CArrayType(elemType))) =>
    //  elements.values forall (includes(_, CPath.Identity, elemType))

    case (JUnionT(ljtpe, rjtpe), (path, ctpe)) => includes(ljtpe, path, ctpe) || includes(rjtpe, path, ctpe)

    case _ => false
  }

  /**
   * Tests whether the supplied sequence contains all the (CPath, CType) pairs that are
   * included by the supplied JType.
   */
  def subsumes(ctpes: Seq[(CPath, CType)], jtpe: JType): Boolean = (jtpe, ctpes) match {
    case (JNumberT, ctpes) => ctpes.exists {
      case (CPath.Identity, CLong | CDouble | CNum) => true
      case _ => false
    }

    case (JTextT, ctpes) => ctpes.exists {
      case (CPath.Identity, CString) => true
      case _ => false
    }

    case (JBooleanT, ctpes) => ctpes.contains(CPath.Identity, CBoolean)

    case (JNullT, ctpes) => ctpes.contains(CPath.Identity, CNull)

    case (JObjectUnfixedT, ctpes) if ctpes.contains(CPath.Identity, CEmptyObject) => true
    case (JObjectUnfixedT, ctpes) => ctpes.exists {
      case (CPath(CPathField(_), _*), _) => true
      case _ => false
    }
    case (JObjectFixedT(fields), ctpes) => {
      val keys = fields.keySet
      keys.forall { key =>
        subsumes(
          ctpes.collect { case (CPath(CPathField(`key`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe) }, 
          fields(key))
      }
    }

    case (JArrayUnfixedT, ctpes) if ctpes.contains(CPath.Identity, CEmptyArray) => true
    case (JArrayUnfixedT, ctpes) => ctpes.exists {
      case (CPath(CPathIndex(_), _*), _) => true
      case _ => false
    }
    case (JArrayFixedT(elements), ctpes) => {
      val indices = elements.keySet
      indices.forall { i =>
        subsumes(
          ctpes.collect {
            case (CPath(CPathIndex(`i`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe)
          }, elements(i))
      }
    }

    case (JUnionT(ljtpe, rjtpe), ctpes) => subsumes(ctpes, ljtpe) || subsumes(ctpes, rjtpe)

    case _ => false
  }
}
