name := "decision-mapper"

version := "1.0"

scalaVersion := "2.12.10"

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "com.typesafe.play" %% "play-json" % "2.9.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.typesafe" % "config" % "1.4.0"
)
