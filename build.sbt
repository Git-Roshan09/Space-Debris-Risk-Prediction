run / fork := true
run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

name := "SpaceDebrisPredictor"

version := "0.1"

scalaVersion := "2.12.18"

// Spark Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",  // Kafka Streaming
  "org.apache.spark" %% "spark-mllib" % "3.5.0"             // MLlib
)

// SGP4 / Orbital Mechanics Dependencies
// We use Orekit, the industry standard Java library for space dynamics
libraryDependencies += "org.orekit" % "orekit" % "12.0"

// Orekit requires a data context (hipparchus)
libraryDependencies += "org.hipparchus" % "hipparchus-core" % "3.0"
libraryDependencies += "org.hipparchus" % "hipparchus-geometry" % "3.0"
libraryDependencies += "org.hipparchus" % "hipparchus-ode" % "3.0"
libraryDependencies += "org.hipparchus" % "hipparchus-fitting" % "3.0"
libraryDependencies += "org.hipparchus" % "hipparchus-optim" % "3.0"

// Repo for Orekit
resolvers += "Orekit Repository" at "https://packages.orekit.org/repository/maven-public/"
