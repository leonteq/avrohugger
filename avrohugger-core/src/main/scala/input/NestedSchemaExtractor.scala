package avrohugger
package input

import avrohugger.matchers.TypeMatcher
import stores.SchemaStore
import types.EnumAsScalaString

import org.apache.avro.Schema
import org.apache.avro.Schema.Type.{ARRAY, ENUM, FIXED, MAP, RECORD, UNION}

import scala.jdk.CollectionConverters._

object NestedSchemaExtractor {
  // if a record is found, extract nested RECORDs and ENUMS (i.e. top-level types) 
  def getNestedSchemas(
                        schema: Schema,
                        schemaStore: SchemaStore,
                        typeMatcher: TypeMatcher): Set[Schema] = {
    def extract(
                 schema: Schema,
                 fieldPath: Set[String] = Set.empty): Set[Schema] = {

      schema.getType match {
        case RECORD =>
          def flattenSchema(fieldSchema: Schema): Set[Schema] = {
            if (fieldPath.contains(fieldSchema.getFullName)) Set(fieldSchema)
            else
              fieldSchema.getType match {
                case ARRAY => flattenSchema(fieldSchema.getElementType)
                case MAP => flattenSchema(fieldSchema.getValueType)
                case RECORD => extract(fieldSchema, fieldPath + fieldSchema.getFullName) + fieldSchema
                case UNION => fieldSchema.getTypes().asScala.toList.flatMap(flattenSchema).toSet
                case ENUM => Set(fieldSchema)
                case FIXED => Set(fieldSchema)
                case _ => Set(fieldSchema)
              }
          }

          def topLevelTypes(schema: Schema) = {
            if (typeMatcher.avroScalaTypes.`enum` == EnumAsScalaString) (schema.getType == RECORD | schema.getType == FIXED)
            else (schema.getType == RECORD | schema.getType == ENUM | schema.getType == FIXED)
          }

          val fields: Set[Schema.Field] = schema.getFields().asScala.toList.toSet
          val fieldSchemas: Set[Schema] = fields.map(field => field.schema)
          fieldSchemas.flatMap(flattenSchema).filter(topLevelTypes)
        case ENUM => Set(schema)
        case FIXED => Set(schema)
        case _ => Set()
      }
    }

    extract(schema) + schema
  }
}

