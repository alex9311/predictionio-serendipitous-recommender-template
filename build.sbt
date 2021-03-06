import AssemblyKeys._

assemblySettings

name := "template-scala-serendipitous"

organization := "io.prediction"

parallelExecution in Test := false

test in assembly := {}

libraryDependencies ++= Seq(
  "io.prediction"    %% "core"          % pioVersion.value % "provided",
  "org.apache.spark" %% "spark-core"    % "1.5.0" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.5.0" % "provided",
  "org.jblas" % "jblas" % "1.2.4",
  "org.scalatest"    %% "scalatest"     % "2.2.1" % "test")
