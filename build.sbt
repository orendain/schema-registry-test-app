name := "schema-registry-test"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.hortonworks.registries" % "schema-registry-serdes" % "0.0.1.3.0.0.0-240"
)
