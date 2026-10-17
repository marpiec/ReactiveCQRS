package io.reactivecqrs.examples

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.jdk.CollectionConverters._

// M7: the book's code is the examples' code. `-Dreactivecqrs.book.write=true` writes the examples' regions into the
// chapters instead of checking them; the diff shows what changed.
class BookSnippetsSpec extends AnyFeatureSpec with Matchers {

  private val sources = Paths.get("examples", "src", "main", "scala", "io", "reactivecqrs", "examples")
  private def chapters: Seq[Path] =
    Files.list(Paths.get("docs", "book")).iterator().asScala.filter(_.toString.endsWith(".md")).toSeq.sortBy(_.toString) :+ Paths.get("README.md")

  Feature("The book's code") {

    Scenario("every code block marked as a snippet is its example's region") {
      if (sys.props.get("reactivecqrs.book.write").contains("true")) BookSnippets.write(chapters, sources)
      BookSnippets.check(chapters, sources) mustBe empty
    }
  }

  Feature("The snippet checker") {

    def file(dir: Path, name: String, lines: String*): Path = {
      val path = dir.resolve(name)
      Files.createDirectories(path.getParent)
      Files.write(path, lines.mkString("\n").getBytes(UTF_8))
    }

    Scenario("finds a block that differs, a region that is missing and a source that is missing, and writes the regions") {
      val dir = Files.createTempDirectory("snippets")
      file(dir, "src/ex/Code.scala",
        "object Code {",
        "  // #outer",
        "  def a = 1",
        "    // #inner",
        "    def b = 2",
        "    // #inner",
        "  // #outer",
        "}")
      val chapter = file(dir, "chapter.md",
        "Text.",
        "",
        "  <!-- snippet: ex/Code.scala#outer -->",
        "  ```scala",
        "  def a = 1",
        "    def b = 2",
        "  ```",
        "",
        "<!-- snippet: ex/Code.scala#inner -->",
        "```scala",
        "def b = 3",
        "```",
        "<!-- snippet: ex/Code.scala#gone -->",
        "```scala",
        "```",
        "<!-- snippet: ex/Other.scala#any -->",
        "```scala",
        "```")
      val src = dir.resolve("src")

      BookSnippets.check(Seq(chapter), src).map(_.replace(chapter.toString, "chapter")) mustBe List(
        "chapter:9: the block differs from ex/Code.scala#inner",
        "chapter:13: ex/Code.scala has no region gone",
        "chapter:16: no example source ex/Other.scala")

      BookSnippets.write(Seq(chapter), src) mustBe Seq(chapter)
      BookSnippets.check(Seq(chapter), src).map(_.replace(chapter.toString, "chapter")) mustBe List(
        "chapter:13: ex/Code.scala has no region gone",
        "chapter:16: no example source ex/Other.scala")
      new String(Files.readAllBytes(chapter), UTF_8).split("\n").slice(9, 12).toSeq mustBe Seq("```scala", "def b = 2", "```")
    }
  }
}
