name := "spark-fhir"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

// https://mvnrepository.com/artifact/com.cerner.bunsen/bunsen-core
libraryDependencies += "com.cerner.bunsen" % "bunsen-core" % "0.4.3"

// https://mvnrepository.com/artifact/com.cerner.bunsen/bunsen-stu3
libraryDependencies += "com.cerner.bunsen" % "bunsen-stu3" % "0.4.3"
