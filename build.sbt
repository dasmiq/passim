name := "passim"

version := "1.0.0"

scalaVersion := "2.10.4"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")
