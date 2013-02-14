package com.precog
package muspelheim 

import yggdrasil._
import yggdrasil.table.cf
import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import scalaz._

trait ParseEvalStack[M[+_]] extends Compiler
    with LineErrors
    with Emitter 
    with StdLibEvaluatorStack[M]

