package Dynamo.Dynamo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import com.google.gson.*; 
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;


import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.GeoPoint;
import com.amazonaws.geo.model.QueryRadiusRequest;
import com.amazonaws.geo.model.QueryRadiusResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Hello world!
 *
 */
public class App 
{	
	static BasicAWSCredentials credentials = new BasicAWSCredentials("XXXXXXXX,XXXXXXXXXXXXXX");
	static AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials);
	static DynamoDB dynamoDB = new DynamoDB(ddb);
	static String tableName = "photo_uploads";
	static Table table = dynamoDB.getTable(tableName);
    public static void main( String[] args )
    {   
    String consumertopic = "searchrequesttopic";
    String consumergroup = "searchrequest";
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "34.193.69.46:9092");
    consumerProps.put("group.id", consumergroup);
    consumerProps.put("enable.auto.commit", "true");
    consumerProps.put("auto.commit.interval.ms", "1000");
    consumerProps.put("session.timeout.ms", "30000");
    consumerProps.put("key.deserializer",          
       "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put("value.deserializer", 
       "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
    
    consumer.subscribe(Arrays.asList(consumertopic));
    
    
    String producerTopic = "facematchtopic";
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", "34.193.69.46:9092");
    producerProps.put("group.id", consumergroup);
    producerProps.put("enable.auto.commit", "true");
    producerProps.put("auto.commit.interval.ms", "1000");
    producerProps.put("session.timeout.ms", "30000");
    producerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    producerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    KafkaProducer<String, String>  producer= new KafkaProducer<String, String>(producerProps);
    
    consumer.subscribe(Arrays.asList(consumertopic));
    System.out.println("Subscribed to topic " + consumertopic);
    int i = 0;
       
    while (true) {
       ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record : records){
             System.out.printf("offset = %d, key = %s, value = %s\n", 
             record.offset(), record.key(), record.value());
          App app=new App();
          JSONObject request=new JSONObject(record.value().toString());
      	Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
          String filterExpression="";
          String[] tags=request.getString("tags").split(",");
          if(tags.length!=0){
          expressionAttributeValues.put(":tag1",tags[0]);
          System.out.println(tags[0]);
          filterExpression=filterExpression+"contains (tags, :tag1)";}
          if (tags.length>1){
          expressionAttributeValues.put(":tag2", tags[1]);
          filterExpression=filterExpression+" OR contains(tags,:tag2)";
          }
          //else{filterExpression=filterExpression+")";}
          if (tags.length>2){
          expressionAttributeValues.put(":tag3",tags[2]);
          filterExpression=filterExpression+" OR contains(tags,:tag3))";
          }
          //else {filterExpression=filterExpression+")";}
      	if(request.get("uploadts").toString()!=""){
      		long start = Long.parseLong(request.get("uploadts").toString())-Long.parseLong(request.get("deviation").toString())*60000;
      		long end = Long.parseLong(request.get("uploadts").toString())+Long.parseLong(request.get("deviation").toString())*60000;

      		expressionAttributeValues.put(":start", String.valueOf(start));
      		expressionAttributeValues.put(":end",String.valueOf(end));
      		if(filterExpression=="")
      		{filterExpression=filterExpression+"rangeKey between :start and :end";}
      		else filterExpression=filterExpression+" AND rangeKey between :start and :end";
      		
      	}
      	System.out.println(filterExpression);
      	HashSet<String> res1 = new HashSet<String>();
      	
      	ItemCollection<ScanOutcome> items = table.scan(
                   filterExpression,//FilterExpression
                  "photourl", //ProjectionExpression
                  null, //ExpressionAttributeNames - not used in this example 
                  expressionAttributeValues);
              
              Iterator<Item> iterator = items.iterator();
              while (iterator.hasNext()) {
                  Item it=iterator.next();
              	res1.add(it.getString("photourl")); 
                  
                  }
               System.out.println(res1);   
              GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(ddb, tableName);
          	GeoDataManager geoDataManager =  new GeoDataManager(config);;

          	
      	double lat = Double.parseDouble(request.get("lat").toString());
      	double lng = Double.parseDouble(request.get("lng").toString());
      	if (lat==0 || lng==0){
      		lat=40.6942036;
      		lng=-73.986579;
      				
      	}
      GeoPoint centerPoint = new GeoPoint(lat, lng);
  	double radiusInMeter = 5000;
  	
  	

  	
  	QueryRadiusRequest queryRadiusRequest = new QueryRadiusRequest(centerPoint, radiusInMeter);
  	QueryRadiusResult queryRadiusResult = geoDataManager.queryRadius(queryRadiusRequest);
  	for (Map<String,AttributeValue> item : queryRadiusResult.getItem()) {
           String currentURL = item.get("photourl").getS();
           if(res1.contains(currentURL)){
              
        	 System.out.println(currentURL);

          	 String email = request.getString("email");
          	 JSONObject eachresult = new JSONObject();
          	 eachresult.put(email,currentURL);
          	 producer.send(new ProducerRecord<String,String>(producerTopic,eachresult.toString()));
    	 
           }
          
  	}
  
          
      }
      
  }
       
 }

   

    	
		
}
