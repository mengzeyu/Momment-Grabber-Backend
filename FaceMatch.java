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
	static BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAJK6B4Z5OSCUMGYMA", "A4piPcEb2aIeirDzv643oJSJrwbKC+un40FSBxVg");
	static AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials);
	static DynamoDB dynamoDB = new DynamoDB(ddb);
	static String tableName = "firebase_device_groups";
	static Table table = dynamoDB.getTable(tableName);
	
   public static void main(String[] args) throws Exception {
    
      
    
	  String topic="facematchtopic";
	   String group="facematch";
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
        	
          	
          
      if(App.FindSimilar(request.getString(email),userID)){
    	  		System.out.println("7"+email);
                HashMap<String, String> nameMap = new HashMap<String, String>();
                nameMap.put("#em", "email");

                HashMap<String, Object> valueMap = new HashMap<String, Object>();
                valueMap.put(":yyyy", email);
                ItemCollection<QueryOutcome> items = null;
                Iterator<Item> iterator=null;
                Item item=null;
                QuerySpec querySpec = new QuerySpec()
                    .withKeyConditionExpression("#em = :yyyy")
                    .withNameMap(nameMap)
                    .withValueMap(valueMap);
                try {
                    System.out.println("result");
                    items = table.query(querySpec);

                    iterator = items.iterator();
                    while (iterator.hasNext()) {
                    	System.out.println("hello");
                        item = iterator.next();
                    	System.out.println(item);

                        String notificationkey=item.getString("notificationkey");
                        System.out.println("4"+notificationkey);
                        URIBuilder builder = new URIBuilder("https://fcm.googleapis.com/fcm/send");
                        URI uri = builder.build();
                        HttpPost post = new HttpPost(uri);
                        post.setHeader("Content-Type", "application/json");
                        post.setHeader("Authorization", "key=AAAA6hRTIho:APA91bEddUj5AhVJNmew-TZhgBUm8p28x4Ff37bAm3DXoQ6fGPQ61VMVKdpzssFwQX3Gq9v79OO1e_MlZuTD56jV3qvwcG7vc__nou96bAacEBbgmtTp5L079h4_O6VExDBAqHUBI1LxGdWNzOn4eHitO_r2wENMTg");
                        JSONObject data=new JSONObject();
                        data.put("Photo Found",request.getString(email));
                        JSONObject notification= new JSONObject();
                        notification.put("title","Photo Found!");
                        notification.put("body",request.getString(email));
                       
                        JSONObject message=new JSONObject();
                       
                        message.put("to", notificationkey);
                        message.put("data",data);
                        message.put("notification",notification);

                        
                        StringEntity reqEntity = new StringEntity(message.toString());
                        post.setEntity(reqEntity);
                        HttpClient httpclient = HttpClients.createDefault();
                        httpclient.execute(post);
                    }

                } catch (Exception e) {
                    System.err.println("Unable to process query ");
                    System.err.println(e.getMessage());
                }
                
               
        	}
            
        }
        
    }
         
   }  }
