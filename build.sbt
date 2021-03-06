name := "spark-salesforce"

version := "2.1.2"

organization := "io.lingk"

scalaVersion := "2.12.13"
crossScalaVersions := Seq("2.11.12", "2.12.13")

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "myget" at "https://www.myget.org/F/salesforce-wave-api/maven"

//resolvers += "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

libraryDependencies ++= Seq(
  "com.force.api" % "force-wsc" % "39.0.1",
  "com.force.api" % "force-partner-api" % "39.0.0",
  "io.lingk" % "salesforce-wave-api" % "1.0.12",
  "org.mockito" % "mockito-core" % "2.2.22"
)

parallelExecution in Test := false

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.8.5"
libraryDependencies += "org.codehaus.woodstox" % "woodstox-core-asl" % "4.4.0"

// Spark Package Details (sbt-spark-package)
spName := "springml/spark-salesforce"

spAppendScalaVersion := true

sparkVersion := "3.0.1"

sparkComponents += "sql"

publishMavenStyle := true

spIncludeMaven := true

spShortDescription := "Spark Salesforce Wave Connector"

spDescription := """Spark Salesforce Wave Connector
                    | - Creates Salesforce Wave Datasets using dataframe
                    | - Constructs Salesforce Wave dataset's metadata using schema present in dataframe
                    | - Can use custom metadata for constructing Salesforce Wave dataset's metadata""".stripMargin

// licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

pomExtra := (
  <url>https://github.com/springml/spark-salesforce</url>
    <licenses>
      <license>
        <name>Apache License, Verision 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <developers>
      <developer>
        <id>springml</id>
        <name>Springml</name>
        <url>http://www.springml.com</url>
      </developer>
    </developers>)


