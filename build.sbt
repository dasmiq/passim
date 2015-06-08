name := "passim"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.1"

resolvers += Resolver.url("edu.umass.ciir.releases", url("http://scm-ciir.cs.umass.edu:8080/nexus/content/repositories/releases"))

resolvers += Resolver.url("edu.umass.ciir.snapshots", url("http://scm-ciir.cs.umass.edu:8080/nexus/content/repositories/snapshots"))

resolvers += Resolver.mavenLocal

libraryDependencies += "org.lemurproject.galago" % "core" % "3.5.2"
libraryDependencies += "org.lemurproject.galago" % "tupleflow" % "3.5.2"
