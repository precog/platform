package com.precog.yggdrasil

import com.precog.common._
import com.precog.util.IOUtils

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer.{pretty,render}
import blueeyes.json.JPath.{JPathDecomposer, JPathExtractor}
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor._
import akka.actor.Actor._
import akka.routing._
import akka.dispatch.Future

import java.io.File

import scala.collection.immutable.ListMap

import scalaz._
import scalaz.effect.IO
import scalaz.Scalaz._
import scalaz.syntax.bifunctor._
import scalaz.ValidationT._

import annotation.tailrec

sealed trait SortBy
case object ById extends SortBy
case object ByValue extends SortBy
case object ByValueThenId extends SortBy

trait SortBySerialization {
  implicit val SortByDecomposer : Decomposer[SortBy] = new Decomposer[SortBy] {
    def decompose(sortBy: SortBy) : JValue = JString(toName(sortBy)) 
  }

  implicit val SortByExtractor : Extractor[SortBy] = new Extractor[SortBy] with ValidatedExtraction[SortBy] {
    override def validated(obj : JValue) : Validation[Error,SortBy] = obj match {
      case JString(s) => fromName(s).map(Success(_)).getOrElse(Failure(Invalid("Unknown SortBy property: " + s))) 
      case _          => Failure(Invalid("Expected JString type for SortBy property"))
    }
  }

  def fromName(s: String): Option[SortBy] = s match {
    case "ById"          => Some(ById)
    case "ByValue"       => Some(ByValue)
    case "ByValueThenId" => Some(ByValueThenId)
    case _               => None
  }

  def toName(sortBy: SortBy): String = sortBy match {
    case ById => "ById"
    case ByValue => "ByValue"
    case ByValueThenId => "ByValueThenId"
  }
}

object SortBy extends SortBySerialization

case class Authorities(uids: Set[String]) {

  @tailrec
  final def hashSeq(l: Seq[String], hash: Int, i: Int = 0): Int = {
    if(i < l.length) {
      hashSeq(l, hash * 31 + l(i).hashCode, i+1)
    } else {
      hash
    }     
  }    

  lazy val hash = {
    if(uids.size == 0) 1 
    else if(uids.size == 1) uids.head.hashCode 
    else hashSeq(uids.toSeq, 1) 
  }

  override def hashCode(): Int = hash
}

trait AuthoritiesSerialization {
  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.uids.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] with ValidatedExtraction[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }
}

object Authorities extends AuthoritiesSerialization 

case class ColumnDescriptor(path: Path, selector: JPath, valueType: CType, authorities: Authorities) {
  lazy val hash = {
    var hash = 1
    hash = hash * 31 + path.hashCode
    hash = hash * 31 + selector.path.hashCode
    hash = hash * 31 + valueType.hashCode
    hash * 31 + authorities.hashCode
  }

  override def hashCode(): Int = hash
  
  override def equals(other: Any): Boolean = other match {
    case o @ ColumnDescriptor(p, s, vt, a) =>
      path == p && selector == s && valueType == vt && authorities == a 
    case _ => false
  }
}

trait ColumnDescriptorSerialization {
  implicit val ColumnDescriptorDecomposer : Decomposer[ColumnDescriptor] = new Decomposer[ColumnDescriptor] {
    def decompose(selector : ColumnDescriptor) : JValue = JObject (
      List(JField("path", selector.path.serialize),
           JField("selector", selector.selector.serialize),
           JField("valueType", selector.valueType.serialize),
           JField("authorities", selector.authorities.serialize))
    )
  }

  implicit val ColumnDescriptorExtractor : Extractor[ColumnDescriptor] = new Extractor[ColumnDescriptor] with ValidatedExtraction[ColumnDescriptor] {
    override def validated(obj : JValue) : Validation[Error,ColumnDescriptor] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "selector").validated[JPath] |@|
       (obj \ "valueType").validated[CType] |@|
       (obj \ "authorities").validated[Authorities]).apply(ColumnDescriptor(_,_,_,_))
  }
}

object ColumnDescriptor extends ColumnDescriptorSerialization 
with ((Path, JPath, CType, Authorities) => ColumnDescriptor)



/** 
 * The descriptor for a projection 
 */
case class ProjectionDescriptor private (identities: Int, indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) {
  lazy val columns = indexedColumns.map(_._1).toList 
  lazy val selectors = columns.map(_.selector).toSet

  def columnAt(path: Path, selector: JPath) = columns.find(col => col.path == path && col.selector == selector)

  def satisfies(col: ColumnDescriptor) = columns.contains(col)

  @tailrec private final def hashIt(cols: List[ColumnDescriptor], hash: Int, i: Int = 0): Int = {
    if(i < cols.length) {
      val col = cols(i)
      hashIt(cols, hash * 31 + cols(i).hashCode, i+1)
    } else {
      hash
    }
  }

  lazy val hash = hashIt(columns, 1)

  override def hashCode: Int = hash 
  
  override def equals(other: Any): Boolean = other match {
    case o @ ProjectionDescriptor(_, _, _) => colsEqual(this, o) && sortEqual(this.sorting, o.sorting)
    case _ => false
  }

  @tailrec
  private final def colsEqual(a: ProjectionDescriptor, b: ProjectionDescriptor, prev: Boolean = true, i: Int = 0): Boolean = {
    if(a.columns.length != b.columns.length) {
      false
    } else if(i < a.columns.length && prev) {
      val cola = a.columns(i)
      val colb = b.columns(i) 
      val local = prev && cola == colb && 
        ((a.indexedColumns.get(cola), b.indexedColumns.get(colb)) match {
          case (None, None) => true
          case (Some(av), Some(bv)) => av == bv
          case _ => false
        })
      
      colsEqual(a,b,local,i+1)
    } else {
      prev      
    }
  }
  
  @tailrec
  private final def sortEqual(a: Seq[(ColumnDescriptor, SortBy)], b: Seq[(ColumnDescriptor, SortBy)], prev: Boolean = true, i: Int = 0): Boolean = {
    if(a.size != b.size) {
      false
    } else if(i < a.length && prev) {
      sortEqual(a,b,prev && a(i) == b(i), i+1)  
    } else {
      prev
    }
  }
}

trait ProjectionDescriptorSerialization {

  case class IndexWrapper(colDesc: ColumnDescriptor, index: Int)

  trait IndexWrapperSerialization {
    implicit val IndexWrapperDecomposer : Decomposer[IndexWrapper] = new Decomposer[IndexWrapper] {
      def decompose(iw: IndexWrapper) : JValue = JObject (
        List(JField("descriptor", iw.colDesc.serialize),
             JField("index", iw.index.serialize))
      )
    }

    implicit val IndexWrapperExtractor : Extractor[IndexWrapper] = new Extractor[IndexWrapper] with ValidatedExtraction[IndexWrapper] {
      override def validated(obj : JValue) : Validation[Error,IndexWrapper] = {
        ((obj \ "descriptor").validated[ColumnDescriptor] |@|
         (obj \ "index").validated[Int]).apply(IndexWrapper(_, _))
      }
    }
  }

  object IndexWrapper extends IndexWrapperSerialization

  case class SortWrapper(colDesc: ColumnDescriptor, sortBy: SortBy)

  trait SortWrapperSerialization {
    implicit val SortWrapperDecomposer : Decomposer[SortWrapper] = new Decomposer[SortWrapper] {
      def decompose(sw: SortWrapper) : JValue = JObject (
        List(JField("descriptor", sw.colDesc.serialize),
             JField("sortBy", sw.sortBy.serialize))
      )
    }

    implicit val SortWrapperExtractor : Extractor[SortWrapper] = new Extractor[SortWrapper] with ValidatedExtraction[SortWrapper] {
      override def validated(obj : JValue) : Validation[Error,SortWrapper] = 
        ((obj \ "descriptor").validated[ColumnDescriptor] |@|
         (obj \ "sortBy").validated[SortBy]).apply(SortWrapper(_, _))
    }
  }

  object SortWrapper extends SortWrapperSerialization

  implicit val ProjectionDescriptorDecomposer : Decomposer[ProjectionDescriptor] = new Decomposer[ProjectionDescriptor] {
    def decompose(pd: ProjectionDescriptor) : JValue = JObject (
      JField("columns", pd.indexedColumns.toList.map( t => IndexWrapper(t._1, t._2) ).serialize) ::
      JField("sorting", pd.sorting.map( t => SortWrapper(t._1, t._2) ).serialize) :: 
      Nil
    )
  } 
  
  implicit val ProjectionDescriptorExtractor : Extractor[ProjectionDescriptor] = new Extractor[ProjectionDescriptor] with ValidatedExtraction[ProjectionDescriptor] { 
    override def validated(obj : JValue) : Validation[Error,ProjectionDescriptor] = 
      ((obj \ "columns").validated[List[IndexWrapper]].map( _.foldLeft(ListMap[ColumnDescriptor, Int]()) { (acc, el) => acc + (el.colDesc -> el.index) }) |@|
       (obj \ "sorting").validated[List[SortWrapper]].map( _.map{ sw => (sw.colDesc, sw.sortBy)})).apply( (_,_) ) match {
         case Success((cols, sorting)) => ProjectionDescriptor(cols, sorting).fold(e => Failure(Invalid(e)), v => Success(v))
         case Failure(e)               => Failure(e)
       }
  } 
}

object ProjectionDescriptor extends ProjectionDescriptorSerialization {

  def trustedApply(identities: Int, indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]): ProjectionDescriptor = ProjectionDescriptor(identities, indexedColumns, sorting)

  def apply(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]): Validation[String, ProjectionDescriptor] = {
    val identities = indexedColumns.values.toSeq.sorted.foldLeft(Option(0)) {
      // test that identities are 0-based and sequential
      case (Some(cur), next) if cur == next => Some(cur + 1)
      case (Some(cur), next) if cur >  next => Some(cur)
      case _ => None
    }

    identities.toSuccess("Column identity indexes must be 0-based and must be sequential when sorted")
    .ensure("A projection may not store values of multiple types for the same selector") { _ =>   
      indexedColumns.keys.groupBy(c => (c.path, c.selector)).values.forall(_.size == 1)
    }

    .ensure("Each identity in a projection may not by shared by column descriptors which sort ById or ByValueThenId more than once") { _ =>
      def sortByIndices(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = {
        indexedColumns.keys.foldLeft(Map.empty: Map[Int, List[SortBy]]) {
          case (indexMap, col) => 
            val sortingMap = sorting.toMap
            if (indexMap.contains(indexedColumns(col))) indexMap + ((indexedColumns(col), indexMap(indexedColumns(col)) :+ sortingMap(col))) 
            else indexMap + ((indexedColumns(col), List(sortingMap(col))))
        }
      }

      indexedColumns.values.toSet.forall(id => 
        (sortByIndices(indexedColumns, sorting)(id).count(a => a == ById || a == ByValueThenId) < 2)
      )
    }

    .map(new ProjectionDescriptor(_, indexedColumns, sorting))
  }

  def toFile(descriptor: ProjectionDescriptor, path: File): IO[Boolean] = {
    IOUtils.safeWriteToFile(pretty(render(descriptor.serialize)), path)
  }

  def fromFile(path: File): IO[Validation[Error,ProjectionDescriptor]] = {
    IOUtils.readFileToString(path).map {
      JsonParser.parse(_).validated[ProjectionDescriptor]
    }
  }
}




trait ByteProjection {
  def descriptor: ProjectionDescriptor

  def project(id: Identities, v: Seq[CValue]): (Array[Byte], Array[Byte]) 
  def unproject(keyBytes: Array[Byte], valueBytes: Array[Byte]): (Identities,Seq[CValue])
  def keyOrder: Order[Array[Byte]]
}

