package io.reactivecqrs.examples

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.jdk.CollectionConverters._

// The plan's link check (M7): every relative link of the repository's Markdown names a file that exists, and every
// #anchor a heading of its file, as GitHub makes them, in both books. Links in code (fenced blocks and `spans`) are not
// links; external ones are not fetched.
class DocLinksSpec extends AnyFeatureSpec with Matchers {

  private val root = Paths.get("").toAbsolutePath // sbt runs the tests in the repository's root

  private def markdownFiles: Seq[Path] = {
    val stream = Files.walk(root)
    try stream.iterator().asScala.filter { path =>
      val relative = root.relativize(path).toString.replace('\\', '/')
      relative.endsWith(".md") && !relative.split('/').exists(part => part == "target" || part.startsWith("."))
    }.toVector finally stream.close()
  }

  private def linesOutsideCode(file: Path): Seq[(String, Int)] = {
    var inFence = false
    Files.readAllLines(file, StandardCharsets.UTF_8).asScala.toSeq.zipWithIndex.flatMap { case (line, i) =>
      if (line.trim.startsWith("```")) { inFence = !inFence; None }
      else if (inFence) None
      else Some((line, i + 1))
    }
  }

  // GitHub's slug of a heading: lower case; letters, digits, spaces, hyphens and underscores kept; spaces to hyphens;
  // a repeated slug gets -1, -2 and so on.
  private def anchorsOf(file: Path): Set[String] = {
    val headings = linesOutsideCode(file).map(_._1).flatMap(line => "^#{1,6}\\s+(.*?)\\s*#*\\s*$".r.findFirstMatchIn(line).map(_.group(1)))
    val slugs = headings.map(h => h.replaceAll("\\[([^\\]]*)\\]\\([^)]*\\)", "$1").trim.toLowerCase
      .replaceAll("[^\\p{L}\\p{N}\\s_-]", "").replaceAll("\\s", "-"))
    val numbered = slugs.foldLeft((Map.empty[String, Int], Vector.empty[String])) { case ((seen, out), slug) =>
      val n = seen.getOrElse(slug, 0)
      (seen.updated(slug, n + 1), out :+ (if (n == 0) slug else slug + "-" + n))
    }._2
    val named = linesOutsideCode(file).flatMap(l => "<a\\s+(?:name|id)=\"([^\"]+)\"".r.findAllMatchIn(l._1).map(_.group(1)))
    (numbered ++ named).toSet
  }

  Feature("The documentation's links") {

    Scenario("every relative link names a file that exists, and every anchor a heading of it") {
      val link = "\\]\\(([^)\\s]+)(?:\\s+\"[^\"]*\")?\\)".r
      val files = markdownFiles
      files.size must be > 30
      val broken = for {
        file <- files
        (line, number) <- linesOutsideCode(file)
        target <- link.findAllMatchIn(line.replaceAll("`[^`]*`", "")).map(_.group(1))
        if !target.matches("^(https?|mailto):.*")
        problem <- {
          val pathPart = target.takeWhile(_ != '#')
          val anchor = Some(target.dropWhile(_ != '#').drop(1)).filter(_.nonEmpty)
          val resolved = if (pathPart.isEmpty) file else file.getParent.resolve(URLDecoder.decode(pathPart, "UTF-8")).normalize()
          if (!Files.exists(resolved)) Some("missing " + target)
          else anchor.filter(a => resolved.toString.endsWith(".md") && !anchorsOf(resolved).contains(a)).map(a => "no heading for #" + a)
        }
      } yield root.relativize(file).toString.replace('\\', '/') + ":" + number + ": " + problem
      broken mustBe empty
    }
  }
}
