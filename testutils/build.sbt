Common.settings("testutils")

import Common.dependencies._

// pekko-cluster for the cluster of one that every spec can run on (M5, G7).
libraryDependencies ++= pekko ++ logback ++ scalikejdbc ++ pekkoCluster.map(_ % Test)
