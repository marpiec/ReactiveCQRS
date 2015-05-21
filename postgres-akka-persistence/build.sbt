Common.settings("postgresAkkaPersistence")

import Common.dependencies._

libraryDependencies ++= akka ++ postgresql ++ scalikejdbc ++ Seq("com.typesafe.akka" %% "akka-testkit" % akkaVersion)