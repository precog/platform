package com.precog.niflheim

import java.io.File

final class CookedBlock(segments: Map[SegmentId, File]) {
}

object CookedBlock {
  def fromFiles(files: Seq[File]): CookedBlock = {
    sys.error("...")
  }
}
