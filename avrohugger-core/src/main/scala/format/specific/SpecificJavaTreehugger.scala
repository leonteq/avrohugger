package avrohugger
package format
package specific

import avrohugger.models.{CompilationUnit, JavaCompilationUnit}
import format.abstractions.JavaTreehugger
import stores.ClassStore
import org.apache.avro.Schema

import java.nio.file.Path

object SpecificJavaTreehugger extends JavaTreehugger {

  def asJavaCodeString(
                        filePath: Option[Path],
                        classStore: ClassStore,
                        namespace: Option[String],
                        schema: Schema): CompilationUnit = {
    JavaCompilationUnit(filePath, schema)
  }

}


