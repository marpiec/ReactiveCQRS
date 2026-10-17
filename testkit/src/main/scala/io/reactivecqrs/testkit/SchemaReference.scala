package io.reactivecqrs.testkit

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import io.mpjsons.MPJsons

import scala.util.{Failure, Success, Try}

/**
 * Guards stored formats (events, saga orders, command responses): one sample per class is compared
 * with a committed reference JSON file named after the class.
 *
 * A class passes when its reference still deserializes to an object equal to the sample (stored
 * data stays readable), and when the sample serialized now deserializes back to itself (new data is
 * readable). Missing references are written in record mode (`-Dreactivecqrs.schema.record=true`)
 * and reported as problems otherwise.
 */
object SchemaReference {

  private val recordMode = sys.props.get("reactivecqrs.schema.record").contains("true")

  /** Returns one problem description per failing sample; empty when every format is unchanged. */
  def verify(mpjsons: MPJsons, referenceDir: Path, samples: Seq[AnyRef]): List[String] = {
    samples.toList.flatMap(sample => verifyOne(mpjsons, referenceDir, sample))
  }

  private def verifyOne(mpjsons: MPJsons, referenceDir: Path, sample: AnyRef): List[String] = {
    val className = sample.getClass.getName
    val referenceFile = referenceDir.resolve(className + ".json")

    Try(mpjsons.serialize(sample, className)) match {
      case Failure(e) => List(s"$className: cannot be serialized: $e")
      case Success(json) =>
        val roundTrip = readsBackAs(mpjsons, json, className, sample, "current output")
        val reference =
          if (Files.exists(referenceFile)) {
            val stored = new String(Files.readAllBytes(referenceFile), StandardCharsets.UTF_8)
            readsBackAs(mpjsons, stored, className, sample, "stored reference " + referenceFile.getFileName)
          } else if (recordMode) {
            Files.createDirectories(referenceDir)
            Files.write(referenceFile, json.getBytes(StandardCharsets.UTF_8))
            Nil
          } else {
            List(s"$className: no reference file $referenceFile (run with -Dreactivecqrs.schema.record=true to create it)")
          }
        roundTrip ++ reference
    }
  }

  private def readsBackAs(mpjsons: MPJsons, json: String, className: String, expected: AnyRef, source: String): List[String] = {
    Try(mpjsons.deserialize[AnyRef](json, className)) match {
      case Success(value) if value == expected => Nil
      case Success(value) => List(s"$className: $source reads back as $value, expected $expected; JSON: $json")
      case Failure(e) => List(s"$className: $source cannot be read: $e; JSON: $json")
    }
  }
}
