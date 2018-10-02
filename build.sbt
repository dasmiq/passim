name := "passim"

version := "0.2.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

resolvers += Resolver.sonatypeRepo("public")

lazy val root = (project in file(".")).
   enablePlugins(BuildInfoPlugin).
   settings(
     buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
     buildInfoPackage := "passim"
   )
