package com.reportgrid.yggdrasil

import scalaz.effect.IO

import java.io.File

package object kafka {
  type ProjectionDescriptorIO = ProjectionDescriptor => IO[Unit] 
  type ProjectionDescriptorLocator = ProjectionDescriptor => IO[File]
}
