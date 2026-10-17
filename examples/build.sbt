Common.settings("examples")

import Common.dependencies._

// The examples of the book (E1-E11, M7): each runnable, tested in `sbt test` with the other suites. Not published.
libraryDependencies ++= pekko ++ pekkoCluster ++ scalikejdbc ++ postgresql ++ Seq("ch.qos.logback" % "logback-classic" % "1.5.32")

publish / skip := true
