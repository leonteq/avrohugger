package avrohugger
package format
package standard

import avrohugger.models.{CompilationUnit, LazyCompilationUnit}
import stores.ClassStore
import format.abstractions.JavaTreehugger
import org.apache.avro.Schema

import java.nio.file.Path
import scala.jdk.CollectionConverters._

object StandardJavaTreehugger extends JavaTreehugger {


  val wrapRegEx = """(.{1,75})\s""".r

  def wrapLine(s: String) = wrapRegEx.replaceAllIn(s, m => m.group(1) + "\n * ")

  def javaDoc(docString: String): String = s"/**${wrapLine(docString)}\n*/"

  def asJavaCodeString(
                        filePath: Option[Path],
                        classStore: ClassStore,
                        namespace: Option[String],
                        schema: Schema): CompilationUnit = {
    schema.getType match {
      case Schema.Type.ENUM =>
        val enumString =
          s"""/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
             |${namespace.orElse(Option(schema.getNamespace)).fold("")(n => s"package $n;")}
             |
             |${Option(schema.getDoc).fold("")(javaDoc)}
             |public enum ${schema.getName} {
             |  ${schema.getEnumSymbols.asScala.mkString(", ")}  ;
             |}""".stripMargin
        LazyCompilationUnit(filePath, enumString)
      case _ => sys.error("Currently ENUM is the only supported Java type.")
    }
  }

}
