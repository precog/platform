package com.precog.yggdrasil
package execution

import com.precog.common._
import com.precog.common.security._
import com.precog.yggdrasil.vfs._

import blueeyes.json._
import scalaz._

trait Platform[M[+_], +A] {
  //def vfs: SecureVFS[M]
  def executorFor(apiKey: APIKey): EitherT[M, String, QueryExecutor[M, A]]
}

