package avrohugger
package models

import avrohugger.format.specific.SpecificJavaTreehugger.{createTempDir, deleteTempDirOnExit, writeJavaTempFile}
import org.apache.avro.Schema

import java.io.{FileNotFoundException, IOException}
import java.nio.file.{Files, Path, StandardOpenOption}

sealed trait CompilationUnit {
  def write(): Unit
}

case class LazyCompilationUnit(maybeFilePath: Option[Path], codeString: String) extends CompilationUnit {
  override def write(): Unit = {
    val path = maybeFilePath match {
      case Some(filePath) => filePath
      case None => sys.error("Cannot write to file without a file path")
    }
    val contents = codeString.getBytes()
    try { // delete old and/or create new
      Files.deleteIfExists(path) // delete file if exists
      Files.createDirectories(path.getParent) // create all parent folders
      Files.write(path, contents, StandardOpenOption.CREATE)
      ()
    }
    catch {
      case ex: FileNotFoundException => sys.error("File not found:" + ex)
      case ex: IOException => sys.error("Problem using the file: " + ex)
    }
  }
}


case class JavaCompilationUnit(maybeFilePath: Option[Path], schema: Schema) extends CompilationUnit {
  override def write(): Unit = {
    // Avro's SpecificCompiler only writes files, but we need a string
    // so write the Java file and read
    val tempDir = createTempDir("avrohugger_specific")
    writeJavaTempFile(schema, tempDir, maybeFilePath.get.getParent.toFile)
    deleteTempDirOnExit(tempDir)
  }
}
