Common.settings("main")

val utils = project.in(file("utils"))
val api = project.in(file("api")).dependsOn(utils)
//val core = project.in(file("core")).dependsOn(api).dependsOn(utils)
//val memory = project.in(file("memory")).dependsOn(core).dependsOn(utils)
//val postgres = project.in(file("postgres")).dependsOn(utils)
//val testdomain = project.in(file("testdomain")).dependsOn(api, core, memory)
val prototype = project.in(file("prototype"))