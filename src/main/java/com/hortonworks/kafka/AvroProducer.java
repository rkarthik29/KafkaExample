package com.hortonworks.kafka;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.JsonObject;
import com.hortonworks.registries.schemaregistry.ConfigEntry;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;


public class AvroProducer {
	
	public static void main(String[] args) throws ExecutionException, InterruptedException {
        String schemaUrl = "http://smunigati-hdf311.field.hortonworks.com:7788/api/v1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "smunigati-hdf311.field.hortonworks.com:6667");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("schema.registry.url", schemaUrl);
        props.put("key.serializer",StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put(SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);
        props.put("logging.level.org.apache.kafka", "DEBUG");
        
        // Hard coding topic too.
        String topic = "users";
        try{
	        JsonObject jsonObject = new JsonObject();
	        jsonObject.addProperty("name", "TestUser");
	        jsonObject.addProperty("age", 39);
	        SchemaRegistryClient client = getSRClient(schemaUrl);
	        SchemaVersionInfo sInfo = client.getLatestSchemaVersionInfo("users");
	        //sInfo.getSchemaText();
	        Schema schema = new Schema.Parser().parse(sInfo.getSchemaText());
	        Producer producer = new KafkaProducer(props);
	        final Callback callback = new MyProducerCallback();
	        for(int i=0;i<100;i++){
	        	Object msg = jsonToAvro(jsonObject.toString(),schema);
		        ProducerRecord record = new ProducerRecord(topic,msg);
		        producer.send(record,callback);
	        }
	        producer.flush();
	        producer.close();
        }catch(Exception ex){
        	ex.printStackTrace();
        }
    }


	private static Object jsonToAvro(String jsonString, Schema schema) throws Exception {
	    DatumReader<Object> reader = new GenericDatumReader(schema);
	    Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonString));
	
	    if (schema.getType().equals(Schema.Type.STRING)) {
	        object = object.toString();
	    }
	    return object;
	}
	
   private static SchemaRegistryClient getSRClient(String schemaUrl){
	   Map<String, Object> config = new HashMap<String,Object>();
	   ConfigEntry entry=null;
       config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaUrl);
       config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
       config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
       config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
       config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
       return new SchemaRegistryClient(config);
   }
   
   private static class MyProducerCallback implements Callback {

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
	           System.out.println("#### received "+metadata+", ex: "+exception+"[{}]");
	}
       
   }

}
