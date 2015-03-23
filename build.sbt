Common.settings("main")

lazy val api = project.in(file("api"))
lazy val core = project.in(file("core")).dependsOn(api)
lazy val memory = project.in(file("memory")).dependsOn(core)
lazy val testdomain = project.in(file("testdomain")).dependsOn(core)