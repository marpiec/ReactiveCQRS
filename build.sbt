Common.settings("main")

val testkit = project.in(file("testkit"))
val api = project.in(file("api"))
val core = project.in(file("core")).dependsOn(api, testkit % "test->test")
//val memory = project.in(file("memory")).dependsOn(core).dependsOn(utils)
//val postgres = project.in(file("postgres")).dependsOn(utils)
val testdomain = project.in(file("testdomain")).dependsOn(api, core, testkit % "test->test")
// Benchmarks and load driver: `sbt "bench/run <report-name>"` (see docs/perf).
val bench = project.in(file("bench")).dependsOn(api, core)
// Test suites of different modules share one local PostgreSQL database.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)
