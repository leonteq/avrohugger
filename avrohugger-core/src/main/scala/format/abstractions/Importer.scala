package avrohugger
package format
package abstractions

import avrohugger.input.{DependencyInspector, NestedSchemaExtractor}
import avrohugger.matchers.TypeMatcher
import avrohugger.matchers.custom.CustomNamespaceMatcher
import avrohugger.stores.SchemaStore
import org.apache.avro.Schema.Type.{ARRAY, _}
import org.apache.avro.{Protocol, Schema}
import treehugger.forest._
import definitions.RootClass
import treehuggerDSL._

import scala.jdk.CollectionConverters._

/** Parent to all ouput formats' importers
  *
  * _ABSTRACT MEMBERS_: to be implemented by a subclass
  * getImports
  *
  * _CONCRETE MEMBERS_: implementations to be inherited by a subclass
  * getEnumSchemas
  * getFieldSchemas
  * getUserDefinedImports
  * getRecordSchemas
  * getTopLevelSchemas
  * isEnum
  * isRecord
  */
trait Importer {

  ///////////////////////////// abstract members ///////////////////////////////
  def getImports(
    schemaOrProtocol: Either[Schema, Protocol],
    currentNamespace: Option[String],
    schemaStore: SchemaStore,
    typeMatcher: TypeMatcher): List[Import]

  ////////////////////////////// concrete members //////////////////////////////
  // gets enum schemas which may be dependencies
  def getEnumSchemas(
    topLevelSchemas: Set[Schema],
    alreadyImported: Set[Schema] = Set.empty[Schema]): Set[Schema] = {
    def nextSchemas(s: Schema, us: Set[Schema]) = getRecordSchemas(Set(s), us)
    topLevelSchemas
      .flatMap(schema => {
        schema.getType match {
          case RECORD =>
            /*val fieldSchemasWithChildSchemas = getFieldSchemas(schema).toSeq
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(s, alreadyImported :+ s))*/
            Seq(schema)/* ++ fieldSchemasWithChildSchemas*/
          case ENUM =>
            Seq(schema)
          case UNION =>
            /*schema.getTypes().asScala
              .find(s => s.getType != NULL).toSeq
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(schema, alreadyImported :+ s))*/
          Nil
          case MAP =>
            /*Seq(schema.getValueType)
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(schema, alreadyImported :+ s))*/Nil
          case ARRAY =>
            /*Seq(schema.getElementType)
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(schema, alreadyImported :+ s))*/Nil
          case _ =>
            Seq.empty[Schema]
        }
      })
      .filter(schema => schema.getType == ENUM)
  }

  def getFixedSchemas(topLevelSchemas: Set[Schema]): Set[Schema] =
    topLevelSchemas
      .filter(schema => schema.getType == FIXED)

//  def getFieldSchemas(schema: Schema): List[Schema] = {
//    schema.getFields().asScala.toList.map(field => field.schema)
//  }

//  def getTypeSchemas(schema: Schema): List[Schema] = {
//    schema.getTypes().asScala.toList
//  }

  def getUserDefinedImports(
    recordSchemas: Set[Schema],
    namespace: Option[String],
    typeMatcher: TypeMatcher): List[Import] = {

    def checkNamespace(schema: Schema): Option[String] = {
      val maybeReferredNamespace =
        DependencyInspector.getReferredNamespace(schema)
      CustomNamespaceMatcher.checkCustomNamespace(
        maybeReferredNamespace,
        typeMatcher,
        maybeDefaultNamespace = maybeReferredNamespace)
    }

    def asImportDef(packageName: String, fields: Set[Schema]): Import = {
      val maybeUpdatedPackageName = CustomNamespaceMatcher.checkCustomNamespace(
        Some(packageName),
        typeMatcher,
        maybeDefaultNamespace = Some(packageName))
      val updatedPkg = maybeUpdatedPackageName.getOrElse(packageName)
      val importedPackageSym = RootClass.newClass(updatedPkg)
      val importedTypes =
        fields.map(field => DependencyInspector.getReferredTypeName(field))
      IMPORT(importedPackageSym, importedTypes)
    }

    def requiresImportDef(schema: Schema): Boolean = {
      (isRecord(schema) || isEnum(schema) || isFixed(schema)) &&
      checkNamespace(schema).isDefined     &&
      checkNamespace(schema) != namespace
    }

    recordSchemas
      .filter(schema => requiresImportDef(schema))
      .groupBy(schema => checkNamespace(schema).getOrElse(schema.getNamespace))
      .toList
      .map(group => group match {
        case(packageName, fields) => asImportDef(packageName, fields)
      })
  }

  // gets record schemas which may be dependencies
  def getRecordSchemas(
    topLevelSchemas: Set[Schema],
    alreadyImported: Set[Schema] = Set.empty[Schema]): Set[Schema] = {
    def nextSchemas(s: Schema, us: Set[Schema]) = getRecordSchemas(Set(s), us)
    topLevelSchemas
      .flatMap(schema => {
        schema.getType match {
          case RECORD =>
            /*val fieldSchemasWithChildSchemas = getFieldSchemas(schema).toSeq
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(s, alreadyImported :+ s))*/
            Seq(schema) /*++ fieldSchemasWithChildSchemas*/
          case ENUM =>
            Seq(schema)
          case UNION =>
            /*schema.getTypes().asScala
              .find(s => s.getType != NULL).toSeq
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(schema, alreadyImported :+ s))*/
            Nil
          case MAP =>
            /*Seq(schema.getValueType)
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(schema, alreadyImported :+ s))*/
            Nil
          case ARRAY =>
            /*Seq(schema.getElementType)
              .filter(s => alreadyImported.contains(s))
              .flatMap(s => nextSchemas(schema, alreadyImported :+ s))*/
            Nil
          case _ =>
            Seq.empty[Schema]
        }
      })
      .filter(schema => isRecord(schema))
  }

  def getTopLevelSchemas(
    schemaOrProtocol: Either[Schema,  Protocol],
    schemaStore: SchemaStore,
    typeMatcher: TypeMatcher): Set[Schema] = {
    schemaOrProtocol match {
      case Left(schema) =>
        blah(schemaStore, typeMatcher, schema)
      case Right(protocol) => protocol.getTypes.asScala.toSet.flatMap(blah(schemaStore, typeMatcher, _))
    }

  }

  private def blah(schemaStore: SchemaStore, typeMatcher: TypeMatcher, schema: Schema) = {
    Set(schema) ++ NestedSchemaExtractor.getNestedSchemas(schema, schemaStore, typeMatcher)
  }

  def isFixed(schema: Schema): Boolean = ( schema.getType == FIXED )

  def isEnum(schema: Schema): Boolean = ( schema.getType == ENUM )

  def isRecord(schema: Schema): Boolean = ( schema.getType == RECORD )

}
