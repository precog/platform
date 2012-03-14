package com.precog
package pandora 

import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

trait ParseEvalStack extends Compiler
    with LineErrors
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with Stdlib
    with MemoryDatasetConsumer 
    with OperationsAPI
