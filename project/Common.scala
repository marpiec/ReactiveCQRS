import sbt.Keys._
import sbt._

object Common {

  val applicationName = "reactivecqrs"

  def settings(moduleName: String) = Seq[Setting[_]](

    name := s"$applicationName-$moduleName",
    version := "0.0.1",
    scalaVersion := "2.11.6",

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

    updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true)

    )

  object dependencies {

    val akkaVersion = "2.3.11"

    val common = Seq(
      "io.mpjsons" %% "mpjsons" % "0.6.7",
      "com.typesafe" % "config" % "1.2.1",
      "org.slf4j" % "slf4j-api" % "1.7.10",
      "org.scalatest" %% "scalatest" % "2.2.4"
    )

    val akka = Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
    )

    val slick = Seq(
      "com.typesafe.slick" %% "slick" % "3.0.0-RC3"
    )

    val scalikejdbc = Seq(
      "org.scalikejdbc" %% "scalikejdbc" % "2.2.6"
    )

    val postgresql = Seq(
      "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
    )

    val logback = Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.3" % Test
    )
  }
}
