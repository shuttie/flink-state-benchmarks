name := "flink-state-bench"

version := "0.1"

scalaVersion := "2.12.10"

lazy val flinkVersion = "1.9.0"

resolvers += Resolver.bintrayRepo("findify", "maven")

libraryDependencies ++= Seq(
  "org.apache.flink"          %% "flink-scala"                % flinkVersion,
  "org.apache.flink"          %% "flink-streaming-scala"      % flinkVersion,
  "io.findify" %% "flink-adt" % "0.1-M13"
)

enablePlugins(JmhPlugin)