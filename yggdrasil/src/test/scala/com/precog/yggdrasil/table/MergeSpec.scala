package com.precog.yggdrasil
package table

import com.precog.bytecode.JType
import com.precog.common.json._
import com.precog.common.security._
import com.precog.yggdrasil.util.IdSourceConfig
import com.precog.yggdrasil.test._

import blueeyes.json._

import java.util.concurrent.Executors

import org.specs2.ScalaCheck
import org.specs2.mutable._

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._

trait MergeSpec[M[+_]] extends
  ColumnarTableModuleTestSupport[M] with
  TableModuleSpec[M] with
  IndicesModule[M] {
  
  type GroupId = Int
  import trans._
  import TableModule._

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def load(apiKey: APIKey, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = M.point(this)
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("todo")
  }
  
  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[M, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1):
        M[(Table, Table)] = sys.error("not implemented here")
  }

  object Table extends TableCompanion
  
  "merge" should {
    "avoid crosses in trivial cases" in {
      val fooJson = """
        | {"key":[5908438637678328470],"value":{"a":0,"b":4}}
        | {"key":[5908438637678328471],"value":{"a":1,"b":5}}
        | {"key":[5908438637678328472],"value":{"a":2,"b":6}}
        | {"key":[5908438637678328473],"value":{"a":3,"b":7}}
        | """.stripMargin
      val foo = fromJson(JParser.parseManyFromString(fooJson).valueOr(throw _).toStream)
    
      val barJson = """
        | {"key":[5908438637678328576],"value":{"a":-1,"c":8,"b":-1}}
        | {"key":[5908438637678328577],"value":{"a":1,"c":9,"b":-1}}
        | {"key":[5908438637678328578],"value":{"a":-1,"c":10,"b":6}}
        | {"key":[5908438637678328579],"value":{"a":3,"c":11,"b":7}}
        | {"key":[5908438637678328580],"value":{"a":0,"c":12,"b":-1}}
        | {"key":[5908438637678328581],"value":{"a":0,"c":13,"b":-1}}
        | """.stripMargin
      val bar = fromJson(JParser.parseManyFromString(barJson).valueOr(throw _).toStream)
    
      val resultJson0 = """
        | {"key":[5908438637678328470,5908438637678328580],"value":{"b":4,"c":12,"a":0,"fa":{"b":4,"a":0}}}
        | {"key":[5908438637678328470,5908438637678328581],"value":{"b":4,"c":13,"a":0,"fa":{"b":4,"a":0}}}
        | {"key":[5908438637678328471,5908438637678328577],"value":{"b":5,"c":9,"a":1,"fa":{"b":5,"a":1}}}
        | {"key":[5908438637678328472,5908438637678328578],"value":{"b":6,"c":10,"a":2,"fa":{"b":6,"a":2}}}
        | {"key":[5908438637678328473,5908438637678328579],"value":{"b":7,"c":11,"a":3,"fa":{"b":7,"a":3}}}
        | """.stripMargin
      val resultJson = JParser.parseManyFromString(resultJson0).valueOr(throw _)
        
      val keyField = CPathField("key")
      val valueField = CPathField("value")
      val aField = CPathField("a")
      val bField = CPathField("b")
      val cField = CPathField("c")
      val oneField = CPathField("1") 
      val twoField = CPathField("2") 
      
      val grouping =
        GroupingAlignment(TransSpec1.Id, TransSpec1.Id,
          GroupingSource(
            bar,
            DerefObjectStatic(Leaf(Source), keyField),
            Some(InnerObjectConcat(ObjectDelete(Leaf(Source), Set(valueField)), WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), valueField), cField), "value"))),
            0,
            GroupKeySpecOr(
              GroupKeySpecSource(oneField, DerefObjectStatic(DerefObjectStatic(Leaf(Source), valueField), aField)),
              GroupKeySpecSource(twoField, DerefObjectStatic(DerefObjectStatic(Leaf(Source), valueField), bField)))
          ),
          GroupingSource(
            foo,
            DerefObjectStatic(Leaf(Source), keyField),
            Some(InnerObjectConcat(ObjectDelete(Leaf(Source),Set(valueField)), WrapObject(DerefObjectStatic(Leaf(Source),valueField),"value"))),
            3,
            GroupKeySpecAnd(
              GroupKeySpecSource(oneField, DerefObjectStatic(DerefObjectStatic(Leaf(Source), valueField), aField)),
              GroupKeySpecSource(twoField, DerefObjectStatic(DerefObjectStatic(Leaf(Source), valueField), bField)))
          ),
          GroupingSpec.Intersection
        )
  
      def evaluator(groupKey: Table, partition: GroupId => M[Table]) = {
        val keyJson = groupKey.toJson.copoint.head
        val K0 = JParser.parse("""{"1":0,"2":4}""")
        val K1 = JParser.parse("""{"1":1,"2":5}""")
        val K2 = JParser.parse("""{"1":2,"2":6}""")
        val K3 = JParser.parse("""{"1":3,"2":7}""")
        
        val r0Json = """
          | {"key":[5908438637678328470,5908438637678328580],"value":{"b":4,"c":12,"a":0,"fa":{"b":4,"a":0}}}
          | {"key":[5908438637678328470,5908438637678328581],"value":{"b":4,"c":13,"a":0,"fa":{"b":4,"a":0}}}
          | """.stripMargin
        val r1Json = """
          | {"key":[5908438637678328471,5908438637678328577],"value":{"b":5,"c":9,"a":1,"fa":{"b":5,"a":1}}}
          """.stripMargin
        val r2Json = """
          | {"key":[5908438637678328472,5908438637678328578],"value":{"b":6,"c":10,"a":2,"fa":{"b":6,"a":2}}}
          | """.stripMargin
        val r3Json = """
          | {"key":[5908438637678328473,5908438637678328579],"value":{"b":7,"c":11,"a":3,"fa":{"b":7,"a":3}}}
          | """.stripMargin
          
        val r0 = fromJson(JParser.parseManyFromString(r0Json).valueOr(throw _).toStream)
        val r1 = fromJson(JParser.parseManyFromString(r1Json).valueOr(throw _).toStream)
        val r2 = fromJson(JParser.parseManyFromString(r2Json).valueOr(throw _).toStream)
        val r3 = fromJson(JParser.parseManyFromString(r3Json).valueOr(throw _).toStream)
          
        (keyJson match {
          case K0 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(3):")
            //partition(3).flatMap(_.toJson).copoint.foreach(println)
            r0
          }
          case K1 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(3):")
            //partition(3).flatMap(_.toJson).copoint.foreach(println)
            r1
          }
          case K2 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(3):")
            //partition(3).flatMap(_.toJson).copoint.foreach(println)
            r2
          }
          case K3 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(3):")
            //partition(3).flatMap(_.toJson).copoint.foreach(println)
            r3
          }
          case _  => sys.error("Unexpected group key")
        }).point[M]
      }
      
      val result = Table.merge(grouping)(evaluator)
      result.flatMap(_.toJson).copoint.toSet must_== resultJson.toSet
    }
    
    "execute the medals query without a cross" in {
      val medalsJson = """
        | {"key":[5908438637678314371],"value":{"Edition":"2000","Gender":"Men"}}
        | {"key":[5908438637678314372],"value":{"Edition":"1996","Gender":"Men"}}
        | {"key":[5908438637678314373],"value":{"Edition":"2008","Gender":"Men"}}
        | {"key":[5908438637678314374],"value":{"Edition":"2004","Gender":"Women"}}
        | {"key":[5908438637678314375],"value":{"Edition":"2000","Gender":"Women"}}
        | {"key":[5908438637678314376],"value":{"Edition":"1996","Gender":"Women"}}
        | {"key":[5908438637678314377],"value":{"Edition":"2008","Gender":"Men"}}
        | {"key":[5908438637678314378],"value":{"Edition":"2004","Gender":"Men"}}
        | {"key":[5908438637678314379],"value":{"Edition":"1996","Gender":"Men"}}
        | {"key":[5908438637678314380],"value":{"Edition":"2008","Gender":"Women"}}
        | """.stripMargin
      val medals = fromJson(JParser.parseManyFromString(medalsJson).valueOr(throw _).toStream)

      val resultJson0 = """
        | {"key":[],"value":{"year":"1996","ratio":139.0}}
        | {"key":[],"value":{"year":"2000","ratio":126.0}}
        | {"key":[],"value":{"year":"2004","ratio":122.0}}
        | {"key":[],"value":{"year":"2008","ratio":119.0}}      
        | """.stripMargin
      val resultJson = JParser.parseManyFromString(resultJson0).valueOr(throw _)
      
      val keyField = CPathField("key")
      val valueField = CPathField("value")
      val genderField = CPathField("Gender")
      val editionField = CPathField("Edition")
      val extra0Field = CPathField("extra0")
      val extra1Field = CPathField("extra1") 
      val oneField = CPathField("1") 

      val grouping =
        GroupingAlignment(TransSpec1.Id, TransSpec1.Id,
          GroupingSource(
            medals,
            DerefObjectStatic(Leaf(Source), keyField),
            Some(InnerObjectConcat(ObjectDelete(Leaf(Source),Set(valueField)), WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),genderField),"value"))),
            0,
            GroupKeySpecAnd(
              GroupKeySpecSource(extra0Field,Filter(EqualLiteral(DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),genderField),CString("Men"),false),EqualLiteral(DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),genderField),CString("Men"),false))),
              GroupKeySpecSource(oneField,DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),editionField)))
          ),
          GroupingSource(
            medals,
            DerefObjectStatic(Leaf(Source),keyField),
            Some(InnerObjectConcat(ObjectDelete(Leaf(Source),Set(valueField)), WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),genderField),"value"))),
            2,
            GroupKeySpecAnd(
              GroupKeySpecSource(extra1Field,Filter(EqualLiteral(DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),genderField),CString("Women"),false),EqualLiteral(DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),genderField),CString("Women"),false))),
              GroupKeySpecSource(oneField,DerefObjectStatic(DerefObjectStatic(Leaf(Source),valueField),editionField)))
          ),
          GroupingSpec.Intersection
        )

      def evaluator(groupKey: Table, partition: GroupId => M[Table]) = {
        val keyJson = groupKey.toJson.copoint.head
        val K0 = JParser.parse("""{"1":"1996","extra0":true,"extra1":true}""")
        val K1 = JParser.parse("""{"1":"2000","extra0":true,"extra1":true}""")
        val K2 = JParser.parse("""{"1":"2004","extra0":true,"extra1":true}""")
        val K3 = JParser.parse("""{"1":"2008","extra0":true,"extra1":true}""")
        
        val r0Json = """
          | {"key":[],"value":{"year":"1996","ratio":139.0}}
          | """.stripMargin
        val r1Json = """
        | {"key":[],"value":{"year":"2000","ratio":126.0}}
          | """.stripMargin
        val r2Json = """
          | {"key":[],"value":{"year":"2004","ratio":122.0}}
          | """.stripMargin
        val r3Json = """
          | {"key":[],"value":{"year":"2008","ratio":119.0}}      
          | """.stripMargin

        val r0 = fromJson(JParser.parseManyFromString(r0Json).valueOr(throw _).toStream)
        val r1 = fromJson(JParser.parseManyFromString(r1Json).valueOr(throw _).toStream)
        val r2 = fromJson(JParser.parseManyFromString(r2Json).valueOr(throw _).toStream)
        val r3 = fromJson(JParser.parseManyFromString(r3Json).valueOr(throw _).toStream)
          

        (keyJson match {
          case K0 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(2):")
            //partition(2).flatMap(_.toJson).copoint.foreach(println)
            r0
          }
          case K1 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(2):")
            //partition(2).flatMap(_.toJson).copoint.foreach(println)
            r1
          }
          case K2 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(2):")
            //partition(2).flatMap(_.toJson).copoint.foreach(println)
            r2
          }
          case K3 => {
            //println("key: "+keyJson+" partition(0):")
            //partition(0).flatMap(_.toJson).copoint.foreach(println)
            //println("key: "+keyJson+" partition(3):")
            //partition(3).flatMap(_.toJson).copoint.foreach(println)
            r3
          }
          case _  => sys.error("Unexpected group key")
        }).point[M]
      }
      
      val result = Table.merge(grouping)(evaluator)
      result.flatMap(_.toJson).copoint.toSet must_== resultJson.toSet
    }
  }
}

object MergeSpec extends MergeSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig with ColumnarTableModuleConfig

  val yggConfig = new IdSourceConfig with ColumnarTableModuleConfig {
    val maxSliceSize = 10
    val smallSliceSize = 3
    
    val idSource = new FreshAtomicIdSource
  }
}
