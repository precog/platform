import sbt._
import Keys._
import AssemblyKeys._
import java.io.File

name := "performance"

version := "0.0.1-SNAPSHOT"

organization := "com.precog"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-g:none")

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
    Seq("-classpath", cpStr) ++
    Seq("specs2.run") ++
    Seq(testName)
  val result = Fork.java.fork(None, opts2, None, Map(), false, LoggedOutput(s.log)).exitValue()
  if (result != 0) error("Tests unsuccessful")    // currently has no effect (https://github.com/etorreborre/specs2/issues/55)
}

libraryDependencies ++= Seq(
  "org.scala-tools.testing"     %% "scalacheck"         % "1.9"            % "test",
  "org.specs2"                  %% "specs2"             % "1.8"            % "test",
  "org.fusesource.leveldbjni"   %  "leveldbjni"         % "1.2-SNAPSHOT"   changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-osx"     % "1.2-SNAPSHOT"   changing(),
  "org.fusesource.leveldbjni"   %  "leveldbjni-linux64" % "1.2-SNAPSHOT"   changing() 
)
