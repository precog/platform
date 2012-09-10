package com.precog.yggdrasil
package table

import org.specs2.mutable.Specification

import blueeyes.json.JPathField
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scalaz._
import scalaz.syntax.copointed._

trait CrossAllSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  import Table._

  override type GroupId = Int

  object crossAllData {
    // These are used in all tests
    val JArray(leftData) = JsonParser.parse("""[
      {
        "groupKeys":  { "001": "foo" },
        "identities": { "1": [1,2] },
        "values":     { "1": { "a": "foo" } }
      }
    ]""")
  
    val JArray(rightData) = JsonParser.parse("""[
      {
        "groupKeys":  { "001": true },
        "identities": { "2": [5,1] },
        "values":     { "2": { "b": true } }
      }
    ]""")
  }

  def simpleCrossAllTest = {
    import crossAllData._
    val varsLeft = Seq(JPathField("a"))
    val varsRight = Seq(JPathField("b"))
  
    val leftBorg = BorgResult(fromJson(leftData.toStream), varsLeft, Set(1))
    val rightBorg = BorgResult(fromJson(rightData.toStream), varsRight, Set(1))

    val expected = (leftData.toStream zip rightData.toStream).map {
      case (lv, rv) => lv.insertAll(rv).getOrElse (throw new Exception("Failed to merge JValues"))
    }

    val result = crossAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== expected
    result.groupKeys must_== varsLeft
    result.groups    must_== Set(1)
  }
}
