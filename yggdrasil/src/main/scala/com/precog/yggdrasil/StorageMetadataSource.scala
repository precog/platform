package com.precog.yggdrasil

import metadata.StorageMetadata
import com.precog.common.security._

trait StorageMetadataSource[M[+_]] {
  def userMetadataView(apiKey: APIKey): StorageMetadata[M]
}

