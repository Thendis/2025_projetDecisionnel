ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15" // "3.3.4"

lazy val root = (project in file("."))
  .settings(
    name := "projetDecisionnel_2025",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.1",
      "org.apache.spark" %% "spark-sql" % "3.3.1",
      "org.postgresql" % "postgresql" % "42.7.5",
      "com.lihaoyi" %% "os-lib" % "0.9.1"
    )
  )
