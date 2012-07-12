package com.precog
package muspelheim 

import yggdrasil._
import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

trait ParseEvalStack extends Compiler
    with LineErrors
    with ProvenanceChecker
    with CriticalConditionFinder
    with Emitter
    with Evaluator
    with StdLib
    with TableModule
