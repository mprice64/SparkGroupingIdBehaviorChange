ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"
// val sparkVersion = "3.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "SparkGroupingIdBehaviorChange",
    idePackagePrefix := Some("dev.mprice")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
