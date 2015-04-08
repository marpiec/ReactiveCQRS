Common.settings("main")

lazy val utils = project.in(file("utils"))
lazy val api = project.in(file("api")).dependsOn(utils)
lazy val core = project.in(file("core")).dependsOn(api).dependsOn(utils)
lazy val memory = project.in(file("memory")).dependsOn(core).dependsOn(utils)
lazy val postgres = project.in(file("postgres")).dependsOn(core).dependsOn(utils)
lazy val testdomain = project.in(file("testdomain")).dependsOn(api, core, memory)