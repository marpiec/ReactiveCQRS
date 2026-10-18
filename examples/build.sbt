Common.settings("examples")

import Common.dependencies._

// The examples of the book (E1-E11, M7): each runnable, tested in `sbt test` with the other suites. Not published.
libraryDependencies ++= pekko ++ pekkoCluster ++ scalikejdbc ++ postgresql ++ Seq("ch.qos.logback" % "logback-classic" % "1.5.32")

// E2's node: Pekko Management's health checks, and the Kubernetes lease of the split brain resolver. Their Pekko modules
// (1.1.5) give way to the library's (1.7.1).
libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-management" % "1.2.1",
  "org.apache.pekko" %% "pekko-lease-kubernetes" % "1.2.1")

publish / skip := true
