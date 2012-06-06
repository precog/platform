name := "bytecode"

version := "0.1.0"

logBuffered := false

publishArtifact in packageDoc := false

initialCommands in console := """
  | import com.precog.bytecode._
  | import java.nio.ByteBuffer
  | 
  | val cake = new BytecodeReader with BytecodeWriter with DAG with util.DAGPrinter
  | 
  | def printBuffer(buffer: ByteBuffer) {
  |   try {
  |     val b = buffer.get()
  |     printf("%02X ", b)
  |   } catch {
  |     case _ => return
  |   }
  |   printBuffer(buffer)
  | }""".stripMargin
