name := "Abhi"

version := "1.0"

scalaVersion := "2.10.4"

// Job Server Dependencies
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.2" % "provided"
libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.6.2" % "provided"
// Spark Dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"

//BerryWorks EDIReader
libraryDependencies += "com.berryworks" % "edireader" % "4.7.3.1"

// https://mvnrepository.com/artifact/xml-apis/xml-apis
libraryDependencies += "xml-apis" % "xml-apis" % "2.0.2"

// https://mvnrepository.com/artifact/javax.xml/jaxp-api
libraryDependencies += "javax.xml.parsers" % "jaxp-api" % "1.4.2"
// https://mvnrepository.com/artifact/com.databricks/spark-xml_2.10
libraryDependencies += "com.databricks" % "spark-xml_2.10" % "0.2.0"
