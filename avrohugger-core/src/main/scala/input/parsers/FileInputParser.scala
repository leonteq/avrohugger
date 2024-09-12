package avrohugger
package input
package parsers

import avrohugger.format.abstractions.SourceFormat
import avrohugger.stores.ClassStore
import org.apache.avro.Schema.Parser
import org.apache.avro.Schema.Type.{ENUM, FIXED, RECORD, UNION}
import org.apache.avro.compiler.idl.Idl
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.{Protocol, Schema, SchemaParseException}

import java.io.File
import scala.jdk.CollectionConverters._
import scala.util.{Right, Try}

case class RecursiveValues(
                            processedFiles: Set[String],
                            processedSchema:Set[Schema],
                            schemaOrProtocols:List[Either[Schema, Protocol]])

class FileInputParser {

  val schemaParser = new Parser()

  def getSchemaOrProtocols(
                            infile: File,
                            format: SourceFormat,
                            classStore: ClassStore,
                            classLoader: ClassLoader,
                            parser: Parser = schemaParser): List[Either[Schema, Protocol]] =
    _getSchemaOrProtocols(
      infile = infile,
        format = format,
        classStore = classStore,
        classLoader = classLoader,
        parser = parser,
      recursiveValues = RecursiveValues(Set.empty,Set.empty,Nil)
    ).schemaOrProtocols

  private def _getSchemaOrProtocols(
    infile: File,
    format: SourceFormat,
    classStore: ClassStore,
    classLoader: ClassLoader,
    parser: Parser,
    recursiveValues: RecursiveValues): RecursiveValues = {
    def unUnion(schema: Schema) = {
      schema.getType match {
        case UNION => schema.getTypes().asScala.toList
        case RECORD => List(schema)
        case ENUM => List(schema)
        case FIXED => List(schema)
        case _ => sys.error("""Neither a record, enum nor a union of either. 
          |Nothing to map to a definition.""".trim.stripMargin)
      }
    }

    def copySchemas(tempParser: Parser, parser: Parser): Unit = {
      val tempKeys = tempParser.getTypes().keySet().asScala
      val keys = parser.getTypes().keySet().asScala
      val commonElements = tempKeys.intersect(keys)
      val nonEqualElements = commonElements.filter { element =>
        parser.getTypes().get(element) != tempParser.getTypes().get(element)
      }
      if (nonEqualElements.nonEmpty) {
        sys.error(s"Can't redefine:  ${nonEqualElements.mkString(",")} in $infile")
      } else {
        if (commonElements.isEmpty) {
          val _ = parser.addTypes(tempParser.getTypes)
        } else {
          val missingTypes = tempParser.getTypes().keySet().asScala.diff(parser.getTypes().keySet().asScala)
          val _ = parser.addTypes(missingTypes.map { t =>
            t -> tempParser.getTypes().get(t)
          }.toMap.asJava)
        }
      }
    }
    
    def mightBeRecoverable(e: SchemaParseException): Boolean = {
      val msg = e.getMessage
      msg.contains("Undefined name:") || msg.contains("is not a defined name") 
    }

    def tryParse(inFile: File, parser: Schema.Parser): List[Schema] = {
      val tempParser = new Parser()
      val parsed = Try(tempParser.parse(inFile)).map(schema => {
        copySchemas(tempParser, parser)
        schema
      }).recoverWith { case e: SchemaParseException if mightBeRecoverable(e) => 
        Try(parser.parse(inFile))
      }
      unUnion(parsed.get)// throw the avro parse exception if Failure
    }

    val fullFileName = infile.getCanonicalPath

    val schemaOrProtocols: RecursiveValues = {

      infile.getName.split("\\.").last match {
        case "avro" =>
          val gdr = new GenericDatumReader[GenericRecord]
          val dfr = new DataFileReader(infile, gdr)
          val schemas = unUnion(dfr.getSchema)
          RecursiveValues(
            processedFiles =  recursiveValues.processedFiles + fullFileName,
              processedSchema = recursiveValues.processedSchema ++ schemas,
              schemaOrProtocols  = recursiveValues.schemaOrProtocols ++ schemas.map(Left(_) )
          )
        case "avsc" =>
          val schemas = tryParse(infile, parser)
          RecursiveValues(
            processedFiles =  recursiveValues.processedFiles + fullFileName,
            processedSchema = recursiveValues.processedSchema ++ schemas,
            schemaOrProtocols  = recursiveValues.schemaOrProtocols ++ schemas.map(Left(_) )
          )
        case "avpr" =>
          val protocol = Protocol.parse(infile)
          RecursiveValues(
            processedFiles =  recursiveValues.processedFiles + fullFileName,
            processedSchema = recursiveValues.processedSchema ++ protocol.getTypes.asScala,
            schemaOrProtocols  = recursiveValues.schemaOrProtocols :+ Right(protocol)
          )
        case "avdl" =>
          val idlParser = new Idl(infile, classLoader)
          val protocol = idlParser.CompilationUnit()
          /**
           * IDLs may refer to types imported from another file. When converted 
           * to protocols, the imported types that share the IDL's namespace 
           * cannot be distinguished from types defined within the IDL, yet 
           * should not be generated as subtypes of the IDL's ADT and should 
           * instead be generated in its own namespace. So, strip the protocol 
           * of all imported types and generate them separately.
           */
          val importedFiles = IdlImportParser.getImportedFiles(infile, classLoader)

          val importedSchemaOrProtocols = importedFiles.foldLeft(recursiveValues)((r, file) => {
            val canonicalPath = file.getCanonicalPath
            if( r.processedFiles.contains(canonicalPath)) {
              r
            } else {
              val importParser = new Parser() // else attempts to redefine schemas
              _getSchemaOrProtocols(file, format, classStore, classLoader, importParser, r)
            }
          })


          val imported = importedSchemaOrProtocols.processedSchema
          val localProtocol = stripImports(protocol, imported)

          RecursiveValues(
            processedFiles =  importedSchemaOrProtocols.processedFiles + fullFileName,
            processedSchema = importedSchemaOrProtocols.processedSchema ++ localProtocol.getTypes.asScala,
            schemaOrProtocols  = importedSchemaOrProtocols.schemaOrProtocols :+ Right(localProtocol)
          )

        case _ =>
          throw new Exception("""File must end in ".avpr" for protocol files, 
            |".avsc" for plain text json files, ".avdl" for IDL files, or .avro 
            |for binary.""".trim.stripMargin)
      }
    }

    schemaOrProtocols
  }

  private def stripImports(protocol: Protocol, importedTypes: Set[Schema]) = {
    val types = protocol.getTypes.asScala.toSet
    val localTypes = types.diff(importedTypes)
    protocol.setTypes(localTypes.asJava)
    protocol
  }
}