import scala.util.Try

Common.settings("bench")

import Common.dependencies._

libraryDependencies ++= pekko ++ scalikejdbc ++ postgresql ++ Seq("ch.qos.logback" % "logback-classic" % "1.5.32")

// A tool, not a library artifact.
publish / skip := true

// Its own JVM, so heap figures are not mixed with sbt's; reports are written under the repository root.
run / fork := true
run / javaOptions ++= Seq("-Xmx2g", s"-Dbench.root=${(ThisBuild / baseDirectory).value}")
run / connectInput := false

// The commit measured, marked when the working tree has uncommitted changes.
run / javaOptions += {
  import scala.sys.process._
  val commit = Try("git rev-parse --short HEAD".!!.trim).getOrElse("unknown")
  val dirty = Try("git status --porcelain".!!.trim.nonEmpty).getOrElse(false)
  s"-Dbench.commit=$commit${if (dirty) " (with uncommitted changes)" else ""}"
}
