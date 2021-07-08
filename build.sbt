name := "passim"

version := "0.2.0"

scalaVersion := "2.12.10"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "graphframes" % "graphframes" % "0.8.0-spark3.0-s_2.12"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

resolvers += Resolver.sonatypeRepo("public")

lazy val root = (project in file(".")).
   enablePlugins(BuildInfoPlugin).
   settings(
     buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
     buildInfoPackage := "passim"
   )
