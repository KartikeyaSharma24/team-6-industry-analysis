ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
lazy val root = (project in file("."))
  .settings(
    name := "Sprint",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql"  % "3.5.0"
    )
  )