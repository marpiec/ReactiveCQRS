Common.settings("testkit")

import Common.dependencies._

// The test kit for applications (E11, R20): published, with no test framework of its own; the library's specs use it
// too, with their ScalaTest base class in its test sources. pekko-cluster for the cluster of one and TestCluster (M5).
libraryDependencies ++= pekko ++ pekkoCluster ++ scalikejdbc ++ postgresql ++ logback
