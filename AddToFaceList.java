package Vision.Vision;
import java.util.Properties;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class test {
	static BasicAWSCredentials credentials = new BasicAWSCredentials("xxxxxxxxxxxxxxxxx", "xxxxxxxxxxxxxxx");
	static AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials);
	static DynamoDB dynamoDB = new DynamoDB(ddb);
	static String tableName = "firebase_device_groups";
	static Table table = dynamoDB.getTable(tableName);
	
   public static void main(String[] args) throws Exception {
    
      
      String topic = "selfietraintopic";
      String group = "selfietrain";
      Properties props = new Properties();
      props.put("bootstrap.servers", "34.193.69.46:9092");
      props.put("group.id", group);
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer",          
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
      
      consumer.subscribe(Arrays.asList(topic));
      System.out.println("Subscribed to topic " + topic);
      int i = 0;
      while (true) {
         ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
               System.out.printf("1.offset = %d, key = %s, value = %s\n", 
               record.offset(), record.key(), record.value());
            App app=new App();
        	JSONObject request=new JSONObject(record.value());
        	String email=request.keys().next();
        	System.out.println("66666");
        	System.out.println("4"+email);
        	String userID = email.substring(0, email.lastIndexOf("@"));
        	
          	
            JSONArray urls=(JSONArray)request.get(email);
        
        	
        	App.createNewFaceList(userID);
        	for(int t=0;t<urls.length();t++)
        	{
        		System.out.println(urls.get(t).toString());
        		App.AddToFaceList(userID.replace(".", ""),urls.get(t).toString());
        	}

        	  
        	 
      
            
        }
        
    }
         
   }  }
