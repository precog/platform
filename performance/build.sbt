import sbt._
import Keys._
import AssemblyKeys._
import java.io.File

name := "performance"

mainTest := "com.precog.performance.PerformanceSuite"

test <<= (streams, fullClasspath in Test, outputStrategy in Test, mainTest) map { (s, cp, os, testName) =>
  val delim = java.io.File.pathSeparator
  val cpStr = cp map { _.data } mkString delim
  s.log.debug("Running with classpath: " + cpStr)
  val opts2 =
    Seq("-server") ++
    Seq("-XX:MaxPermSize=512m") ++
    Seq("-Xms3G") ++
    Seq("-Xmx4G") ++
    Seq("-XX:+UseConcMarkSweepGC") ++
    Seq("-XX:+CMSIncrementalMode") ++
    Seq("-XX:-CMSIncrementalPacing") ++ 
    Seq("-XX:CMSIncrementalDutyCycle=100") ++
    Seq("-classpath", cpStr) ++
    Seq("specs2.run") ++
    Seq(testName)
  val result = Fork.java.fork(None, opts2, None, Map(), false, LoggedOutput(s.log)).exitValue()
  if (result != 0) error("Tests unsuccessful")    // currently has no effect (https://github.com/etorreborre/specs2/issues/55)
}

libraryDependencies ++= Seq(
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.2-SNAPSHOT"   changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-osx"     % "1.2-SNAPSHOT"   changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.2-SNAPSHOT"   changing() 
)
