package com.precog
package util

import java.io.File

import org.specs2.mutable._

class IOUtilsSpecs extends Specification {
  "IOUtils" should {
    "properly clean empty directories recursively" in {
      val tmpRoot = IOUtils.createTmpDir("IOUtilsSpecs").unsafePerformIO

      val rootParent = tmpRoot.getParentFile

      val child1 = new File(tmpRoot, "child1")
      val child2 = new File(tmpRoot, "child2")

      val gchild1_1 = new File(child1, "child1")
      val gchild1_2 = new File(child1, "child2")

      val gchild2_1 = new File(child2, "child1")

      List(child1, child2, gchild1_1, gchild1_2, gchild2_1).foreach(_.mkdir)

      // This should fail because tmpRoot has children
      IOUtils.recursiveDeleteEmptyDirs(tmpRoot, rootParent).unsafePerformIO

      tmpRoot.isDirectory mustEqual true

      // This should delete both gchild2_1 and child2, but leave tmpRoot
      IOUtils.recursiveDeleteEmptyDirs(gchild2_1, rootParent).unsafePerformIO

      gchild2_1.isDirectory mustEqual false
      child2.isDirectory mustEqual false
      tmpRoot.isDirectory mustEqual true

      // This should delete gchild1_1 and leave child1 and gchild1_2
      IOUtils.recursiveDeleteEmptyDirs(gchild1_1, rootParent).unsafePerformIO

      gchild1_1.isDirectory mustEqual false
      gchild1_2.isDirectory mustEqual true
      child1.isDirectory mustEqual true
      tmpRoot.isDirectory mustEqual true

      // This should not do anything because it's specified as the root
      IOUtils.recursiveDeleteEmptyDirs(tmpRoot, tmpRoot).unsafePerformIO

      tmpRoot.isDirectory mustEqual true

      // cleanup
      IOUtils.recursiveDelete(tmpRoot).unsafePerformIO

      tmpRoot.isDirectory mustEqual false
    }
  }
}
