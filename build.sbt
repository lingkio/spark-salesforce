name := "spark-salesforce"

version := "4.0.3"

organization := "io.lingk"

scalaVersion := "2.12.20"
crossScalaVersions := Seq("2.12.20")

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "myget" at "https://www.myget.org/F/salesforce-wave-api/maven"
resolvers += Resolver.mavenLocal
resolvers += "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

libraryDependencies ++= Seq(
  "com.force.api" % "force-wsc" % "39.0.1",
  "com.force.api" % "force-partner-api" % "39.0.0",
  "io.lingk" % "salesforce-wave-api" % "3.0.3",
  "org.mockito" % "mockito-core" % "5.0.0"
)

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4"

parallelExecution in Test := false

//resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += Resolver.url("artifactory", url("https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
resolvers += Resolver.url("sbt-maven-releases", url("https://repo.scala-sbt.org/scalasbt/maven-releases"))(Resolver.ivyStylePatterns)
resolvers += Resolver.url("sbt-maven-snapshots", url("https://repo.scala-sbt.org/scalasbt/maven-snapshots"))(Resolver.ivyStylePatterns)
//resolvers += Resolver.url("Maven Central", url("https://repo1.maven.org/maven2/"))(Resolver.ivyStylePatterns)
//resolvers ++= Seq(
//  "sbt-maven-releases"  at "https://repo.scala-sbt.org/scalasbt/maven-releases",
//  "sbt-maven-snapshots" at "https://repo.scala-sbt.org/scalasbt/maven-snapshots",
//  "Maven Central"       at "https://repo1.maven.org/maven2/"
//)(Resolver.ivyStylePatterns)

//resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/"
)
resolvers += Resolver.mavenLocal

//updateOptions := updateOptions.value.withChecksums(false)
libraryDependencies += "io.lingk" % "salesforce-wave-api" % "3.0.3" from "file:///Users/mahendrabagul/DevEnv/lingk/repos/salesforce-wave-api/target/salesforce-wave-api-3.0.3.jar"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.2" % "test"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.9.10"
libraryDependencies += "com.fasterxml.woodstox" % "woodstox-core" % "5.3.0"

// Spark Package Details (sbt-spark-package)
spName := "springml/spark-salesforce"

spAppendScalaVersion := true

sparkVersion := "3.5.3"

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


