package com.precog.common

import org.specs2.mutable.Specification

class CPathSpec extends Specification {
  import CPath._

  "makeTree" should {
    "return correct tree given a sequence of CPath" in {
      val cpaths: Seq[CPath] = Seq(
        CPath(CPathField("foo")),
        CPath(CPathField("bar"), CPathIndex(0)),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("baz")),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("ack")),
        CPath(CPathField("bar"), CPathIndex(2)))

      val values: Seq[Int] = Seq(4, 6, 7, 2, 0)

      val result = makeTree(cpaths, values)

      val expected: CPathTree[Int] = {
        RootNode(Seq(
          FieldNode(CPathField("bar"), 
            Seq(
              IndexNode(CPathIndex(0), Seq(LeafNode(4))),  
              IndexNode(CPathIndex(1), 
                Seq(
                  FieldNode(CPathField("ack"), Seq(LeafNode(6))),  
                  FieldNode(CPathField("baz"), Seq(LeafNode(7))))),  
              IndexNode(CPathIndex(2), Seq(LeafNode(2))))),  
          FieldNode(CPathField("foo"), Seq(LeafNode(0)))))
      }

      result mustEqual expected
    }
  }
}
