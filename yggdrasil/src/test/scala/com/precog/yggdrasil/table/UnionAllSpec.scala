package com.precog.yggdrasil
package table

import org.specs2.mutable.Specification

import blueeyes.json.JPathField
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scalaz._
import scalaz.syntax.copointed._

trait UnionAllSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  import Table._

  override type GroupId = Int

  object unionAllData {
    val JArray(leftData) = JsonParser.parse("""[
      {
        "groupKeys":  { "%1$s": "foo", "%2$s": false },
        "identities": { "1": [1,2] },
        "values":     { "1": { "a": "foo", "b": false } }
      },
      {
        "groupKeys":  { "%1$s": "foo", "%2$s": true },
        "identities": { "1": [1,3] },
        "values":     { "1": { "a": "foo", "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
  
    val JArray(rightData) = JsonParser.parse("""[
      {
        "groupKeys":  { "%1$s": "bar", "%2$s": true },
        "identities": { "1": [5,1] },
        "values":     { "1": { "a": "bar", "b": true } }
      },
      {
        "groupKeys":  { "%1$s": "baz", "%2$s": true },
        "identities": { "1": [4,5] },
        "values":     { "1": { "a": "baz", "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
  
    val JArray(rightDataReversed) = JsonParser.parse("""[
      {
        "groupKeys":  { "%2$s": "bar", "%1$s": true },
        "identities": { "1": [5,1] },
        "values":     { "1": { "a": "bar", "b": true } }
      },
      {
        "groupKeys":  { "%2$s": "baz", "%1$s": true },
        "identities": { "1": [4,5] },
        "values":     { "1": { "a": "baz", "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
  }

  def simpleUnionAllTest = {
    import unionAllData._
    val vars = Seq(JPathField("a"), JPathField("b"))
  
    val leftBorg = BorgResult(fromJson(leftData.toStream), vars, Set(1))
    val rightBorg = BorgResult(fromJson(rightData.toStream), vars, Set(1))

    val expected = leftData.toStream ++ rightData.toStream

    val result = unionAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== expected
    result.groupKeys must_== vars
    result.groups    must_== Set(1)
  }

  def reversedUnionAllTest = {
    import unionAllData._
    val varsLeft = Seq(JPathField("a"), JPathField("b"))
    val varsRight = Seq(JPathField("b"), JPathField("a"))
  
    val leftBorg = BorgResult(fromJson(leftData.toStream), varsLeft, Set(1))
    val rightBorg = BorgResult(fromJson(rightDataReversed.toStream), varsRight, Set(1))

    val expected = leftData.toStream ++ rightData.toStream

    val result = unionAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== expected
    result.groupKeys must_== varsLeft
    result.groups    must_== Set(1)
  }
}
