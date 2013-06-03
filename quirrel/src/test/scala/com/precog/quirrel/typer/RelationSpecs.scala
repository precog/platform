package com.precog
package quirrel
package typer

import com.codecommit.gll.LineStream
import org.specs2.mutable.Specification

import java.io.File
import scala.io.Source

object RelationSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker
    with StaticLibrarySpec {

  import ast._
  
  "explicit relation" should {
    "fail on natively-related sets" in {
      {
        val tree = compileSingle("//a ~ //a 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = compileSingle("1 ~ 2 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = compileSingle("a := new 1 a ~ a 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
    }
    
    "fail on explicitly related sets" in {
      val tree = compileSingle("a := //a b := //b a ~ b a ~ b 42")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(AlreadyRelatedSets)
    }
    
    "accept object definition on different loads when related" in {
      val tree @ Relate(_, _, _, in) = compileSingle("//foo ~ //bar { a: //foo, b: //bar }")
      in.relations mustEqual Map(StaticProvenance("/foo") -> Set(StaticProvenance("/bar")), StaticProvenance("/bar") -> Set(StaticProvenance("/foo")))
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept object definition on static and dynamic provenances when related" in {
      val tree @ Let(_, _, _, _, Relate(_, _, _, in)) = compileSingle("s := new 1 //foo ~ s { a: //foo, b: s }")
      (in.relations.keys.toList sorted Provenance.order.toScalaOrdering) must beLike { case DynamicProvenance(_) :: StaticProvenance("/foo") :: Nil => ok }
      in.relations(StaticProvenance("/foo")).toList(0) must beLike { case DynamicProvenance(_) => ok }
      (in.relations.values.map(_.toList)) must beLike { case (DynamicProvenance(_) :: Nil) :: (StaticProvenance("/foo") :: Nil) :: Nil => ok }
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 { a: s1, b: s2 }")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on different loads when related" in {
      val tree @ Relate(_, _, _, in) = compileSingle("//foo ~ //bar [ //foo, //bar ]")
      in.relations mustEqual Map(StaticProvenance("/foo") -> Set(StaticProvenance("/bar")), StaticProvenance("/bar") -> Set(StaticProvenance("/foo")))
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept array definition on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s [ //foo, s ]")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 [ s1, s2 ]")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo[//bar]")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept deref on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo[s]")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1[s2]")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept dispatch on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar fun(a, b) := a + b fun(//foo, //bar)")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty
    }
    
    "accept dispatch on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s fun(a, b) := a + b fun(//foo, s)")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 fun(a, b) := a + b fun(s1, s2)")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept where on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo where //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept where on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo where s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept where on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 where s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }    
    "accept with on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo with //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept with on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo with s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept with on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 with s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }    
    "accept union on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo union //bar")
      tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
      tree.errors must beEmpty      
    }
    
    "accept union on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo union s")
      tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept union on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 union s2")
      tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }    

    "accept intersect on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo intersect //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }
    
    "accept intersect on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo intersect s")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }
    
    "accept intersect on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 intersect s2")
      tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }    
    
    "accept difference on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo difference //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "accept difference on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo difference s")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "accept difference on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 difference s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo + //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept addition on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo + s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 + s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo - //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept subtraction on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo - s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 - s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo * //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept multiplication on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo * s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 * s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo / //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept division on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo / s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 / s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo < //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept less-than on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo < s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 < s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo <= //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo <= s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 <= s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo > //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept greater-than on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo > s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 > s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo >= //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo >= s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 >= s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo = //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept equality on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo = s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 = s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo != //bar")
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty      
    }
    
    "accept not-equality on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo != s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept not-equality on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 != s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept boolean and on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo & //bar")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo & s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept boolean and on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 & s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept boolean or on different loads when related" in {
      val tree = compileSingle("//foo ~ //bar //foo | //bar")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on static and dynamic provenances when related" in {
      val tree = compileSingle("s := new 1 //foo ~ s //foo | s")
      tree.provenance must beLike { case ProductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on differing dynamic provenances when related" in {
      val tree = compileSingle("s1 := new 1 s2 := new 1 s1 ~ s2 s1 | s2")
      tree.provenance must beLike { case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "reject addition with unrelated relation" in {
      val tree = compileSingle("//a ~ //b //c + //d")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "substitute and resolve relation unification within a function" in {
      val tree = compileSingle("f(a, b) := a ~ b a + b f(//foo, //bar)")
      tree.errors must beEmpty
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
    }
    
    "accept operations according to the commutative relation" in {
      {
        val input = """
          | foo := //foo
          | bar := //bar
          | 
          | foo ~ bar
          |   foo + bar""".stripMargin
          
        val tree = compileSingle(input)
        tree.provenance must beLike {
          case p: ProductProvenance => {
            p.possibilities must contain(StaticProvenance("/foo"))
            p.possibilities must contain(StaticProvenance("/bar"))
          }
        }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | foo := //foo
          | bar := //bar
          | 
          | foo ~ bar
          |   bar + foo""".stripMargin
          
        val tree = compileSingle(input)
        tree.provenance must beLike {
          case p: ProductProvenance => {
            p.possibilities must contain(StaticProvenance("/foo"))
            p.possibilities must contain(StaticProvenance("/bar"))
          }
        }
        tree.errors must beEmpty
      }
    }
    
    "accept operations according to the transitive relation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | foo ~ bar
        |   bar ~ baz
        |     foo + baz""".stripMargin
        
      val tree = compileSingle(input)
      tree.provenance must beLike {
          case p: ProductProvenance => {
            p.possibilities must contain(StaticProvenance("/foo"))
            p.possibilities must contain(StaticProvenance("/baz"))
          }
        }
      tree.errors must beEmpty
    }
    
    "accept operations according to the commutative-transitive relation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | foo ~ bar
        |   bar ~ baz
        |     baz + foo""".stripMargin
        
      val tree = compileSingle(input)
      tree.provenance must beLike {
          case p: ProductProvenance => {
            p.possibilities must contain(StaticProvenance("/foo"))
            p.possibilities must contain(StaticProvenance("/baz"))
          }
        }
      tree.errors must beEmpty
    }
    
    "accept multiple nested expressions in relation" in {
      {
        val input = """
        | foo := //foo
        | bar := //bar
        | 
        | foo ~ bar
        |   foo + bar + foo""".stripMargin
        
        val tree @ Let(_, _, _, _, Let(_, _, _, _, Relate(_, _, _, in))) = compileSingle(input)
        in.relations mustEqual Map(StaticProvenance("/foo") -> Set(StaticProvenance("/bar")), StaticProvenance("/bar") -> Set(StaticProvenance("/foo")))
        tree.provenance must beLike {
          case p: ProductProvenance => {
            p.possibilities must contain(StaticProvenance("/foo"))
            p.possibilities must contain(StaticProvenance("/bar"))
          }
        }
        tree.errors must beEmpty
      }
      
      {
        val input = """
        | foo := //foo
        | bar := //bar
        | 
        | foo ~ bar
        |   bar + foo + foo""".stripMargin
        
        val tree = compileSingle(input)
        tree.provenance must beLike {
          case p: ProductProvenance => {
            p.possibilities must contain(StaticProvenance("/foo"))
            p.possibilities must contain(StaticProvenance("/bar"))
          }
        }
        tree.errors must beEmpty
      }
    }
    
    "attribute union provenance to constituents in trinary operation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        |
        | foo ~ bar
        |   bar ~ baz
        |     (foo.a - bar.a) * (bar.b / baz.b)
        """.stripMargin
        
      val tree @ Let(_, _, _, _, Let(_, _, _, _, Let(_, _, _, _, Relate(_, _, _, Relate(_, _, _, body @ Mul(_, left @ Sub(_, minLeft, minRight), right @ Div(_, divLeft, divRight))))))) =
        compileSingle(input)

      body.relations mustEqual Map(
        StaticProvenance("/foo") -> Set(StaticProvenance("/bar")),
        StaticProvenance("/bar") -> Set(StaticProvenance("/foo"), StaticProvenance("/baz")),
        StaticProvenance("/baz") -> Set(StaticProvenance("/bar")))
      
      tree.provenance must beLike {
        case p: ProductProvenance => {
          p.possibilities must contain(StaticProvenance("/foo"))
          p.possibilities must contain(StaticProvenance("/bar"))
          p.possibilities must contain(StaticProvenance("/baz"))
        }
      }
      tree.errors must beEmpty
      
      body.provenance must beLike {
        case p: ProductProvenance => {
          p.possibilities must contain(StaticProvenance("/foo"))
          p.possibilities must contain(StaticProvenance("/bar"))
          p.possibilities must contain(StaticProvenance("/baz"))
        }
      }
      
      body.provenance.possibilities must contain(StaticProvenance("/foo"))
      body.provenance.possibilities must contain(StaticProvenance("/bar"))
      body.provenance.possibilities must contain(StaticProvenance("/baz"))
      
      left.provenance.possibilities must contain(StaticProvenance("/foo"))
      left.provenance.possibilities must contain(StaticProvenance("/bar"))
      left.provenance.possibilities must not(contain(StaticProvenance("/baz")))
      
      right.provenance.possibilities must not(contain(StaticProvenance("/foo")))
      right.provenance.possibilities must contain(StaticProvenance("/bar"))
      right.provenance.possibilities must contain(StaticProvenance("/baz"))
      
      minLeft.provenance mustEqual StaticProvenance("/foo")
      minRight.provenance mustEqual StaticProvenance("/bar")
      
      divLeft.provenance mustEqual StaticProvenance("/bar")
      divRight.provenance mustEqual StaticProvenance("/baz")
    }
  }
}
