name := "passim"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0"

resolvers += Resolver.mavenLocal

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")
