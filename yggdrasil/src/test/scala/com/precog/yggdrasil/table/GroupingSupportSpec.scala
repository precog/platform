package com.precog.yggdrasil
package table

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait GroupingSupportSpec[M] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
    import Table._
    import Table.Universe._
    /*
    import OrderingConstraint2._

    def c(str: String) = OrderingConstraint2.parse(str)

    object ConstraintParser extends scala.util.parsing.combinator.JavaTokenParsers {
      lazy val ticVar: Parser[OrderingConstraint2] = "'" ~> ident ^^ (s => Variable(JPathField(s)))

      lazy val ordered: Parser[OrderingConstraint2] = ("[" ~> repsep(constraint, ",") <~ "]") ^^ (v => Ordered(v.toSeq))

      lazy val unordered: Parser[OrderingConstraint2] = ("{" ~> repsep(constraint, ",") <~ "}") ^^  (v => Unordered(v.toSet))

      lazy val zero: Parser[OrderingConstraint2] = "*" ^^^ Zero

      lazy val constraint: Parser[OrderingConstraint2] = ticVar | ordered | unordered | zero

      def parse(input: String): OrderingConstraint2 = parseAll(constraint, input).getOrElse(sys.error("Could not parse " + input))
    }

    def parse(input: String): OrderingConstraint2 = ConstraintParser.parse(input)

    "ordering constraint 2" >> {
      import OrderingConstraint2.{Join, Ordered, Unordered, Zero, Variable}

      "parse" should {
        "accept tic variable" in {
          c("'a").render mustEqual ("'a")
        }

        "accept empty seq" in {
          c("[]").render mustEqual ("[]")
        }

        "accept seq of tic variables" in {
          c("['a, 'b, 'c]").render mustEqual ("['a, 'b, 'c]")
        }

        "accept empty set" in {
          c("{}").render mustEqual ("{}")
        }

        "accept set of tic variables" in {
          c("{'a}").render mustEqual ("{'a}")
        }

        "accept sets and seqs combined" in {
          c("['a, ['b, {'d}], {'c}]").render mustEqual ("['a, ['b, {'d}], {'c}]") 
        }
      }

      "normalize" should {
        "remove empty seqs" in {
          c("['a, [], 'b]").normalize.render mustEqual "['a, 'b]"
        }

        "remove empty sets" in {
          c("['a, {}, 'b]").normalize.render mustEqual "['a, 'b]"
        }

        "collapse singleton seqs" in {
          c("['a, ['c], 'b]").normalize.render mustEqual "['a, 'c, 'b]"
        }

        "collapse singleton sets" in {
          c("['a, {'c}, 'b]").normalize.render mustEqual "['a, 'c, 'b]"
        }

        "collapse multiple level seqs" in {
          c("['a, [[['c]]], 'b]").normalize.render mustEqual "['a, 'c, 'b]"
        }

        "collapse multiple level sets" in {
          c("['a, {{{'c}}}, 'b]").normalize.render mustEqual "['a, 'c, 'b]"
        }

        "convert empty seq to zero" in {
          c("[[[]]]").normalize.render mustEqual "*"
        }

        "convert empty set to zero" in {
          c("{{{}}}").normalize.render mustEqual "*"
        }
      }

      "variables" should {
        "identify all variables" in {
          c("[{'a, 'b}, 'c, ['d, {'e, 'f}]]").variables mustEqual Set("a", "b", "c", "d", "e", "f").map(JPathField.apply)
        }
      }

      "-" should {
        "remove vars from nested expression" in {
          (c("[{'a, 'b}, 'c, ['d, {'e, 'f}]]") - c("['a, 'f]").fixed.toSet).render mustEqual c("['b, 'c, 'd, 'e]").render
        }
      }

      "join" should {
      //  "succeed for ['a, 'b] join ['a]" in {
      //    c("['a, 'b]").join(c("['a]")) mustEqual Join(c("'a"), leftRem = c("'b"), rightRem = Zero)
      //  }

      //  "succeed for ['a, 'b] join {'a}" in {
      //    c("['a, 'b]").join(c("{'a}")) mustEqual Join(c("'a"), leftRem = c("'b"), rightRem = Zero)
      //  }

      //  "succeed for ['a, 'b] join {'a, 'b}" in {
      //    c("['a, 'b]").join(c("{'a, 'b}")) mustEqual Join(c("['a, 'b]"), leftRem = Zero, rightRem = Zero)
      //  }

      //  "fail for ['a, 'b] join ['b]" in {
      //    c("['a, 'b]").join(c("['b]")) mustEqual Join(Zero, c("['a, 'b]"), c("'b"))
      //  }

      //  "succeed for ['a, 'b, 'c] join [{'a, 'b}, 'c]" in {
      //    c("['a, 'b, 'c]").join(c("[{'a, 'b}, 'c]")) mustEqual Join(c("['a, 'b, 'c]"))
      //  }

      //  "succeed for ['a, 'b, 'c] join [{'a, 'b}, 'd]" in {
      //    c("['a, 'b, 'c]").join(c("[{'a, 'b}, 'd]")) mustEqual Join(c("['a, 'b]"), leftRem = c("'c"), rightRem = c("'d"))
      //  }

      //  "succeed for ['b, 'c] join 'b" in {
      //    c("['b, 'c]").join(c("'b")) mustEqual Join(join = c("'b"), leftRem = c("'c"), rightRem = Zero)
      //  }

        "succeed for {'a, ['b, 'c], 'd} join {'a, 'b, ['c, 'd]}" in {
          val joined = c("{'a, ['b, 'c], 'd}").join(c("{'a, 'b, ['c, 'd]}"))

          println("join = " + joined.join.render + ", leftRem = " + joined.leftRem.render + ", rightRem = " + joined.rightRem.render)

          // join = 'a, leftRem = {'d, ['b, 'c]}, rightRem = {'b, ['c, 'd]}

          joined mustEqual Join(c("{'a, ['b, 'c, 'd]}"))
        }.pendingUntilFixed

      //  "succeed for {'a, 'b} join {'a, 'b, 'c}" in {
      //    c("{'a, 'b}").join(c("{'a, 'b, 'c}")) mustEqual Join(join = c("{'a, 'b}"), leftRem = Zero, rightRem = c("'c"))
      //  }
      }
    }

    "borg algorithm" >> {
      //val plan1 = BorgTraversalPlanUnfixed()
      "unfixed plan" should {
          val ab = mergeNode("ab")
          val abc = mergeNode("abc")
          val ad = mergeNode("ad")

          val connectedNodes = Set(ab, abc, ad)

          val spanningGraph = findSpanningGraphs(edgeMap(connectedNodes)).head

          val oracle = Map(
            ab -> NodeMetadata(10, None),
            abc -> NodeMetadata(10, None),
            ad -> NodeMetadata(10, None)
          )

          //val plan = findBorgTraversalOrder(spanningGraph, oracle)

          val steps = Vector.empty[BorgTraversalPlanUnfixed] //plan.unpack

//          val nodeOrder = steps.map(_.nodeOrder.render)
//          val accOrderPre = steps.map(_.accOrderPre.render)
//          val accOrderPost = steps.map(_.accOrderPost.render)
//          val accResort = steps.map(_.accResort)
//          val nodeResort = steps.map(_.nodeResort)


          steps.foreach { plan =>
            println("=========================")
            println("node = " + plan.node)
            println("nodeOrder = " + plan.nodeOrder.render)
            println("accOrderPre = " + plan.accOrderPre.render)
            println("accOrderPost = " + plan.accOrderPost.render)
            println("accResort = " + plan.accResort)
            println("nodeResort = " + plan.nodeResort)
          }
      }

//      "traversal ordering" >> {
//        "for ab-abc-ad" should {
//          val ab = mergeNode("ab")
//          val abc = mergeNode("abc")
//          val ad = mergeNode("ad")
//
//          val connectedNodes = Set(ab, abc, ad)
//
//          val spanningGraph = findSpanningGraphs(edgeMap(connectedNodes)).head
//
//          val oracle = Map(
//            ab -> NodeMetadata(10, None),
//            abc -> NodeMetadata(10, None),
//            ad -> NodeMetadata(10, None)
//          )
//
//          val plan = findBorgTraversalOrder(spanningGraph, oracle).fixed
//
//          val nodes = plan.steps.map(_.node)
//
//          val accOrdersPre = plan.steps.map(_.accOrderPre)
//          val accOrdersPost = plan.steps.map(_.accOrderPost)
//          val nodeOrders = plan.steps.map(_.nodeOrder)
//          val resorts = plan.steps.map(_.accResort)
//
//          val ad_abc_ab = Vector(ad, abc, ab)
//          val ad_ab_abc = Vector(ad, ab, abc)
//          val ab_abc_ad = Vector(ab, abc, ad)
//          val abc_ab_ad = Vector(abc, ab, ad)
//          val abc_ad_ab = Vector(abc, ad, ab)
//          val ab_ad_abc = Vector(ab, ad, abc)
//
//          //println(nodes)
//
//          "choose correct node order" in {
//            ((nodes == ad_abc_ab) ||
//             (nodes == ad_ab_abc) ||
//             (nodes == ab_abc_ad) ||
//             (nodes == ab_ad_abc) ||
//             (nodes == abc_ad_ab) ||
//             (nodes == abc_ab_ad)) must beTrue
//          }
//
//          "not require any acc resorts" in {
//            forall(resorts) { resort =>
//              resort must beFalse
//            }
//          }
//
//          "correctly order acc post and pre" in {
//            nodes match {
//              case `ad_abc_ab` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ad"), order("abdc"))
//                accOrdersPost must_== Vector(order("ad"), order("abdc"), order("abdc"))
//
//              case `ad_ab_abc` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ad"), order("abd"))
//                accOrdersPost must_== Vector(order("ad"), order("abd"), order("abcd"))
//
//              case `ab_abc_ad` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ab"), order("abc"))
//                accOrdersPost must_== Vector(order("ab"), order("abc"), order("abcd"))
//
//              case `ab_ad_abc` =>
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ab"), order("abd"))
//                accOrdersPost must_== Vector(order("ab"), order("abd"), order("abcd"))
//
//              case `abc_ab_ad` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("abc"), order("abc"))
//                accOrdersPost must_== Vector(order("abc"), order("abc"), order("abcd"))
//
//              case `abc_ad_ab` => 
//                accOrdersPre must_== Vector(OrderingConstraint.Zero, order("abc"), order("abcd"))
//                accOrdersPost must_== Vector(order("abc"), order("abcd"), order("abcd"))
//            }
//          }
//
//          "correctly order nodes" in {
//            nodes match {
//              case `ad_abc_ab` => 
//                nodeOrders must_== Vector(order("ad"), order("abc"), order("ab"))
//
//              case `ab_abc_ad` => 
//                nodeOrders must_== Vector(order("ab"), order("abc"), order("ad"))
//                
//              case `abc_ab_ad` => 
//                nodeOrders must_== Vector(order("abc"), order("ab"), order("ad"))
//
//              case `abc_ad_ab` =>
//                nodeOrders must_== Vector(order("abc"), order("ad"), order("ab"))
//
//              case `ab_ad_abc` => 
//                nodeOrders must_== Vector(order("ab"), order("ad"), order("abc"))
//            }
//          }
//        }
//      }
//    }
*/
}


// vim: set ts=4 sw=4 et:
