ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.1"

libraryDependencies ++= Seq(
  "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1"
)

lazy val root = (project in file("."))
  .settings(
    name := "untitled4"
  )
