Common.settings("main")

val testutils = project.in(file("testutils"))
val api = project.in(file("api"))
val core = project.in(file("core")).dependsOn(api, testutils % "test->test")
//val memory = project.in(file("memory")).dependsOn(core).dependsOn(utils)
//val postgres = project.in(file("postgres")).dependsOn(utils)
val testdomain = project.in(file("testdomain")).dependsOn(api, core, testutils  % "test->test")
// Test suites of different modules share one local PostgreSQL database.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)
