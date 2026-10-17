package io.reactivecqrs.examples

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

/**
  * The book's code is the examples' code (M7). A region of an example source between two lines `// #name` is a snippet;
  * a code block of a chapter right after a line `<!-- snippet: e1/BasicSetup.scala#wiring -->` (a path under the
  * examples' sources) must be that region, without the indentation the region's lines share. Regions may nest; marker
  * lines belong to none. `check` lists the blocks that differ, `write` puts the regions into the chapters.
  */
object BookSnippets {

  private val marker = """^\s*<!-- snippet: ([\w./-]+)#([\w-]+) -->\s*$""".r
  private val regionLine = """^\s*// #([\w-]+)\s*$""".r
  private val fence = """^(\s*)```scala\s*$""".r

  /** The regions of a source, by name, each without the indentation its lines share. */
  def regions(source: Seq[String]): Map[String, Seq[String]] = {
    var open = Map[String, Vector[String]]()
    var closed = Map[String, Seq[String]]()
    source.foreach {
      case regionLine(name) if open.contains(name) =>
        closed += name -> dedented(open(name))
        open -= name
      case regionLine(name) => open += name -> Vector.empty
      case line => open = open.map { case (name, lines) => name -> (lines :+ line) }
    }
    closed ++ open.map { case (name, _) => name -> Nil } // an unclosed region shows as empty, so its blocks differ
  }

  private def dedented(lines: Seq[String]): Seq[String] = {
    val indent = lines.filter(_.trim.nonEmpty).map(_.takeWhile(_ == ' ').length).minOption.getOrElse(0)
    lines.map(line => if (line.trim.isEmpty) "" else line.drop(indent))
  }

  /** A marked block of a chapter: the marker's line (from 1), the snippet it names, and the lines of its code. */
  private case class Block(file: Path, line: Int, snippet: String, name: String, indent: String, from: Int, until: Int)

  private def blocks(file: Path, lines: Seq[String]): Either[String, Seq[Block]] = {
    val found = lines.indices.collect { case i if marker.matches(lines(i)) =>
      val marker(snippet, name) = lines(i): @unchecked
      lines.lift(i + 1) match {
        case Some(fence(indent)) =>
          val end = lines.indexWhere(_.trim == "```", i + 2)
          if (end < 0) Left(s"$file:${i + 1}: the code block of $snippet#$name has no end")
          else Right(Block(file, i + 1, snippet, name, indent, i + 2, end))
        case _ => Left(s"$file:${i + 1}: no scala code block follows the marker of $snippet#$name")
      }
    }
    found.collectFirst { case Left(problem) => problem }.toLeft(found.collect { case Right(block) => block })
  }

  private def read(file: Path): Seq[String] = new String(Files.readAllBytes(file), UTF_8).split("\r?\n", -1).toSeq

  private def snippet(sources: Path, block: Block): Either[String, Seq[String]] = {
    val source = sources.resolve(block.snippet)
    if (!Files.exists(source)) Left(s"${block.file}:${block.line}: no example source ${block.snippet}")
    else regions(read(source)).get(block.name).toRight(s"${block.file}:${block.line}: ${block.snippet} has no region ${block.name}")
  }

  /** The marked blocks of `chapters` that are not their snippets, each named with its chapter and line. */
  def check(chapters: Seq[Path], sources: Path): List[String] = chapters.toList.flatMap { chapter =>
    val lines = read(chapter)
    blocks(chapter, lines) match {
      case Left(problem) => List(problem)
      case Right(found) => found.toList.flatMap { block =>
        snippet(sources, block) match {
          case Left(problem) => List(problem)
          case Right(code) =>
            val shown = lines.slice(block.from, block.until).map(_.stripPrefix(block.indent))
            if (shown == code) Nil
            else List(s"${block.file}:${block.line}: the block differs from ${block.snippet}#${block.name}")
        }
      }
    }
  }

  /** Puts each marked block's snippet into its chapter; a block whose snippet is missing stays as it is. Returns the
    * chapters it changed. */
  def write(chapters: Seq[Path], sources: Path): Seq[Path] = chapters.filter { chapter =>
    val text = new String(Files.readAllBytes(chapter), UTF_8)
    val lineEnd = if (text.contains("\r\n")) "\r\n" else "\n"
    val lines = read(chapter)
    val rewritten = blocks(chapter, lines).toOption.getOrElse(Nil).sortBy(-_.from).foldLeft(lines) { (current, block) =>
      snippet(sources, block) match {
        case Right(code) => current.patch(block.from, code.map(line => if (line.isEmpty) "" else block.indent + line), block.until - block.from)
        case Left(_) => current
      }
    }
    val changed = rewritten != lines
    if (changed) Files.write(chapter, rewritten.mkString(lineEnd).getBytes(UTF_8))
    changed
  }
}
