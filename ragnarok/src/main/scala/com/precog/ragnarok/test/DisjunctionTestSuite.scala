package com.precog
package ragnarok
package test

object DisjunctionTestSuite extends PerfTestSuite {
  query(
    """
      | foo := //foo
      | bar := //bar
      | solve 'a = foo.a, 'b = foo.b
      |   c' := bar.c where bar.a = 'a | bar.b = 'b
      |   { a: 'a, b: 'b, c: c' }
      |""".stripMargin)


//      | foo := //foo
//      | bar := //bar
//      | solve 'a, 'b
//      |   foo' := foo where foo.a = 'a & foo.b = 'b
//      |   c' := bar.c where bar.a = 'a | bar.b = 'b
//      |   foo' ~ c'
//      |   { a: 'a, b: 'b, c: c', fa: foo' }

      //      | foo := //foo
//      | solve 'a = foo.a, 'b = foo.b
//      |   { a: 'a, b: 'b }

//      | foo := //foo
//      | solve 'a, 'b
//      |   foo' := foo where foo.a = 'a & foo.b = 'b
//      |   { a: foo'.a, b: foo'.b }

//      | foo := //foo
//      | { a: foo.a, b: foo.b }

      
//      | foo := //foo
//      | bar := //bar
//      | solve 'a = foo.a, 'b = foo.b
//      |   c' := bar.c where bar.a = 'a | bar.b = 'b
//      |   { a: 'a, b: 'b, c: c' }

//      | foo := //foo
//      | bar := //bar
//      | solve 'a, 'b
//      | foo' := foo where foo.a = 'a & foo.b = 'b
//      | bar' := bar where bar.a = 'a
//      |   ['a, 'b, sum(foo'.a), sum(bar'.a)]
      
}
