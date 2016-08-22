name := "passim"

version := "1.0.0-alpha.1"

scalaVersion := "2.10.5"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")

lazy val root = (project in file(".")).
   enablePlugins(BuildInfoPlugin).
   settings(
     buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
     buildInfoPackage := "passim"
   )
