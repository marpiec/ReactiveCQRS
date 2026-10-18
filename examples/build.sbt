Common.settings("examples")

import Common.dependencies._

// The examples of the book (E1-E11, M7): each runnable, tested in `sbt test` with the other suites. Not published.
libraryDependencies ++= pekko ++ pekkoCluster ++ scalikejdbc ++ postgresql ++ Seq("ch.qos.logback" % "logback-classic" % "1.5.32")

// E2's node: Pekko Management's health checks and Cluster Bootstrap (F180), its discovery of the contact points on
// Kubernetes, and the Kubernetes lease of the split brain resolver. Their Pekko modules (1.1.5) give way to the
// library's (1.7.1); pekko-discovery is named to keep it on that version too.
libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-management" % "1.2.1",
  "org.apache.pekko" %% "pekko-management-cluster-bootstrap" % "1.2.1",
  "org.apache.pekko" %% "pekko-discovery-kubernetes-api" % "1.2.1",
  "org.apache.pekko" %% "pekko-lease-kubernetes" % "1.2.1",
  "org.apache.pekko" %% "pekko-discovery" % pekkoVersion)

// E2's image (examples/deploy/e2): the examples' runtime jars in target/e2/lib, which the Dockerfile copies, so the
// image needs neither sbt nor the dependencies' repositories.
lazy val stageE2 = taskKey[File]("Copies the examples' runtime jars into target/e2/lib for E2's image")
stageE2 := {
  val lib = target.value / "e2" / "lib"
  IO.delete(lib)
  (Runtime / fullClasspathAsJars).value.map(_.data).foreach(jar => IO.copyFile(jar, lib / jar.getName))
  lib.getParentFile
}

publish / skip := true
