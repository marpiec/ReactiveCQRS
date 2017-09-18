import sbt.Keys._
import sbt._

object Common {

  def settings(moduleName: String) = Seq[Setting[_]](

    organization := "io.reactivecqrs",
    name := s"reactivecqrs-$moduleName",
    version := "0.10.7",
    scalaVersion := "2.11.7",

    /* required for Scalate to avoid version mismatch */
    dependencyOverrides := Set(
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
      "-target:jvm-1.8",
      "-encoding", "UTF-8"),

    resolvers in ThisBuild ++= Seq(
      "eclipse repo" at "https://repo.eclipse.org/content/groups/releases/",
      "Sonatype repo" at "https://oss.sonatype.org/content/repositories/releases/",
      "marpiec BinTray" at "https://bintray.com/artifact/download/marpiec/maven/"),

    libraryDependencies ++= dependencies.common,

    licenses += ("MIT", url("http://opensource.org/licenses/MIT")),

    updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true),

    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { _ => false },

    publishLocal := {},

    publishTo := Some("snapshots" at sys.props.getOrElse("snapshotsRepo", default = "http://someMockRepo.com"))

  )

  object dependencies {

    val akkaVersion = "2.3.12"

    val common = Seq(
      "io.mpjsons" %% "mpjsons" % "0.6.12",
      "com.typesafe" % "config" % "1.2.1",
      "org.slf4j" % "slf4j-api" % "1.7.10",
      "org.scalatest" %% "scalatest" % "2.2.4" % Test
    )

    val akka = Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
    )

    val scalikejdbc = Seq(
      "org.scalikejdbc" %% "scalikejdbc" % "2.2.6"
    )

    val postgresql = Seq(
      "org.postgresql" % "postgresql" % "9.4-1201-jdbc41"
    )

    val logback = Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.3" % Test
    )
  }
}
