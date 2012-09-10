package com.precog.yggdrasil
package table

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import blueeyes.json.JPathField
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scalaz._
import scalaz.syntax.copointed._

trait UnionAllSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import Table._

  override type GroupId = Int

  def simpleUnionAllTest = {
    val JArray(left) = JsonParser.parse("""[
      {
        "groupKeys":  { "001": "foo", "002": false },
        "identities": { "1": [1,2] },
        "values":     { "1": { "a": "foo", "b": false } }
      },
      {
        "groupKeys":  { "001": "foo", "002": true },
        "identities": { "1": [1,3] },
        "values":     { "1": { "a": "foo", "b": true } }
      }
    ]""")

    val JArray(right) = JsonParser.parse("""[
      {
        "groupKeys":  { "001": "bar", "002": true },
        "identities": { "1": [5,1] },
        "values":     { "1": { "a": "bar", "b": true } }
      },
      {
        "groupKeys":  { "001": "baz", "002": true },
        "identities": { "1": [4,5] },
        "values":     { "1": { "a": "baz", "b": true } }
      }
    ]""")

    val vars = Seq(JPathField("a"), JPathField("b"))
  
    val leftBorg = BorgResult(fromJson(left.toStream), vars, Set(1))
    val rightBorg = BorgResult(fromJson(right.toStream), vars, Set(1))

    val expected = left.toStream ++ right.toStream

    val result = unionAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== expected
    result.groupKeys must_== vars
    result.groups    must_== Set(1)
  }
}
