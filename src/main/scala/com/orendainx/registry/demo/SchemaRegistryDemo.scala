package com.orendainx.registry.demo

import java.io.ByteArrayInputStream

import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient
import com.hortonworks.registries.schemaregistry.serdes.avro.{AvroSnapshotDeserializer, AvroSnapshotSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.collection.JavaConverters._

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
object SchemaRegistryDemo extends App {

  val stringToSerialize = "1488947606535|19|1|Edgar Orendain|100|Des Moines to Chicago|41.68932225997044|-93.36181640625|100|Normal|0|0|0"

  /*
   * Setup
   */
  val clientConfig = Map(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name() -> "http://sandbox.hortonworks.com:25105/api/v1").asJava
  val schemaRegistryClient = new SchemaRegistryClient(clientConfig)

  // EnrichedTruckData schema pasted at the end of this file for reference
  val enrichedDataSchemaMetadata = schemaRegistryClient.getSchemaMetadataInfo("EnrichedTruckData").getSchemaMetadata
  val enrichedDataSchemaInfo = schemaRegistryClient.getLatestSchemaVersionInfo("EnrichedTruckData")

  val serializer = schemaRegistryClient.getDefaultSerializer(AvroSchemaProvider.TYPE).asInstanceOf[AvroSnapshotSerializer]
  val deserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE).asInstanceOf[AvroSnapshotDeserializer]

  serializer.init(clientConfig)
  deserializer.init(clientConfig)


  val record = createGenericRecord()
  val serializedRecord = serializer.serialize(record, enrichedDataSchemaMetadata)
  val deserializedRecord = deserializeBytes(serializedRecord)

  //println(s"Original string: $stringToSerialize")
  println(s"Original record: $record")
  println(s"Serialized record: ${new String(serializedRecord)}")
  println(s"Deserialized record: $deserializedRecord")

  val reserializedRecord = serializer.serialize(deserializedRecord, enrichedDataSchemaMetadata)
  println(s"Reserialized, once deserialized, record: ${new String(reserializedRecord)}")

  val deserializedObj = deserializedRecord.asInstanceOf[GenericData.Record]
  println(s"DeserializedObj $deserializedObj")


  def deserializeBytes(bytes: Array[Byte]) = {
    val byteStream = new ByteArrayInputStream(bytes)
    //deserializer.deserialize(byteStream, enrichedDataSchemaMetadata, null)
    //deserializer.deserialize(byteStream, enrichedDataSchemaMetadata)
    deserializer.deserialize(byteStream, 1)
  }

  private def createGenericRecord() = {
    val record = new GenericData.Record(new Schema.Parser().parse(enrichedDataSchemaInfo.getSchemaText))
    record.put("eventTime", 1488947606535L)
    record.put("truckId", 19)
    record.put("driverId", 1)
    record.put("driverName", "Edgar Orendain")
    record.put("routeId", 100)
    record.put("routeName", "Des Moines to Chicago")
    record.put("latitude", 41.68932225997044)
    record.put("longitude", -93.36181640625)
    record.put("speed", 100)
    record.put("eventType", "Normal")
    record.put("foggy", 0)
    record.put("rainy", 0)
    record.put("windy", 0)
    record
  }
}

/* For reference, EnrichedTruckData schema looks like:
{
 "type": "record",
 "namespace": "com.orendainx.hortonworks.trucking",
 "name": "EnrichedTruckData",
 "fields": [
  {
   "name": "eventTime",
   "type": "long"
  },
  {
   "name": "truckId",
   "type": "int"
  },
  {
   "name": "driverId",
   "type": "int"
  },
  {
   "name": "driverName",
   "type": "string"
  },
  {
   "name": "routeId",
   "type": "int"
  },
  {
   "name": "routeName",
   "type": "string"
  },
  {
   "name": "latitude",
   "type": "double"
  },
  {
   "name": "longitude",
   "type": "double"
  },
  {
   "name": "speed",
   "type": "int"
  },
  {
   "name": "eventType",
   "type": "string"
  },
  {
   "name": "foggy",
   "type": "int"
  },
  {
   "name": "rainy",
   "type": "int"
  },
  {
   "name": "windy",
   "type": "int"
  }
 ]
}
*/
