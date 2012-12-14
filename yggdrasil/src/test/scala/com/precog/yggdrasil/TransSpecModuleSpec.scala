package com.precog.yggdrasil

import test._
import table._

import com.precog.common.json._

import org.specs2.mutable.Specification

trait TransSpecModuleSpec extends TransSpecModule with FNDummyModule with Specification {
  import trans._
  import CPath._

  "concatChildren" should {
    "transform a CPathTree into a TransSpec" in {
      val tree: CPathTree[Int] = RootNode(Seq(
        FieldNode(CPathField("bar"), 
          Seq(
            IndexNode(CPathIndex(0), Seq(LeafNode(4))),  
            IndexNode(CPathIndex(1), Seq(FieldNode(CPathField("baz"), Seq(LeafNode(6))))),  
            IndexNode(CPathIndex(2), Seq(LeafNode(2))))),  
        FieldNode(CPathField("foo"), Seq(LeafNode(0)))))

      val result = TransSpec.concatChildren(tree)

      def leafSpec(idx: Int) = DerefArrayStatic(Leaf(Source), CPathIndex(idx))

      val expected = InnerObjectConcat(
        WrapObject(
          ArrayConcat(
            ArrayConcat(
              WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(4))), 
              WrapArray(WrapObject(DerefArrayStatic(Leaf(Source),CPathIndex(6)),"baz"))), 
            WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(2)))),"bar"), 
          WrapObject(
            DerefArrayStatic(Leaf(Source),CPathIndex(0)),"foo"))

      result mustEqual expected
    }
  }
}

object TransSpecModuleSpec extends TransSpecModuleSpec
