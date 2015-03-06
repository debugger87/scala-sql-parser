import AssemblyKeys._

name := "scala-sql-parser"

version := "0.1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "org.specs2" %% "specs2" % "2.3.12" % "test"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

resolvers ++= Seq(
  "maven.mei.fm" at "http://maven.mei.fm/nexus/content/groups/public/"
)

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case m if m.toLowerCase.matches("meta-inf.*\\.mf$") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.rsa$") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.dsa$") => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}

net.virtualvoid.sbt.graph.Plugin.graphSettings