package com.precog.yggdrasil
package execution

import com.precog.common._
import com.precog.common.security._
import com.precog.yggdrasil.vfs._

import blueeyes.json._
import scalaz._

trait Execution[M[+_], +A] {
  def executorFor(apiKey: APIKey): EitherT[M, String, QueryExecutor[M, A]]
}

trait Platform[M[+_], Block, +A] extends Execution[M, A] with SecureVFSModule[M, Block] {
  def vfs: SecureVFS
}

