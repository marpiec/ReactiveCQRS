import sbt.*
import sbt.Keys.*

object Common {

  def settings(moduleName: String) = Seq[Setting[_]](

    version := "1.0.0-SNAPSHOT",

    organization := "io.reactivecqrs",
    name := s"reactivecqrs-$moduleName",
    scalaVersion := "2.13.18",

    // required for Scalate to avoid version mismatch
    dependencyOverrides := Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value
    ),

    scalacOptions ++= Seq(
      "-feature",
      "-unchecked",
      "-deprecation",
      "-Xlint",
      "-Ywarn-dead-code",
      "-language:_",
      "-release", "17",
      "-encoding", "UTF-8"
    ),

    ThisBuild / resolvers ++= Seq(
      Resolver.mavenLocal,
      "eclipse repo" at "https://repo.eclipse.org/content/groups/releases/",
      "Sonatype repo" at "https://oss.sonatype.org/content/repositories/releases/",
      "marpiec BinTray" at "https://bintray.com/artifact/download/marpiec/maven/"
    ),

    libraryDependencies ++= dependencies.common,

    // Suites share one local PostgreSQL database; running them one at a time avoids concurrent
    // schema initialization and interference between suites.
    Test / parallelExecution := false,

    licenses += ("MIT", url("http://opensource.org/licenses/MIT")),

    updateOptions := updateOptions.value.withCachedResolution(true),

    publishMavenStyle := true,
    Test / publishArtifact := false,

    pomIncludeRepository := { _ => false },

    publishTo := Some(
      "snapshots" at sys.props.getOrElse(
        "snapshotsRepo",
        "https://nexus.neula.in/repository/neula-releases"
      )
    )

  )

  object dependencies {

    val pekkoVersion = "1.7.1"

    val common = Seq(
      "io.mpjsons" %% "mpjsons" % "0.7.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )

    val pekko = Seq(
      "org.apache.pekko" %% "pekko-actor" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % Test
    )

    // M5: a cluster of one on-premise, of many in the cloud (D42); sharded owners (D43), singletons (D46, D53).
    val pekkoCluster = Seq(
      "org.apache.pekko" %% "pekko-cluster" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-tools" % pekkoVersion,
      "org.apache.pekko" %% "pekko-coordination" % pekkoVersion
    )

    val scalikejdbc = Seq(
      "org.scalikejdbc" %% "scalikejdbc" % "4.3.5",
      "org.scalikejdbc" %% "scalikejdbc-streams" % "4.3.5"
    )

    val postgresql = Seq(
      "org.postgresql" % "postgresql" % "42.7.14"
    )

    val logback = Seq(
      "ch.qos.logback" % "logback-classic" % "1.5.32" % Test
    )
  }
}
