package Vision.Vision;


import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.google.gson.*; 
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

public class App {
	static String APIkey="XXXXXXXXXXXXXX";
	
	
	public static void createNewFaceList(String userID) 
    {
       HttpClient httpclient = HttpClients.createDefault();
        try
        {
            URIBuilder builder = new URIBuilder("https://api.projectoxford.ai/face/v1.0/facelists/"+userID.toLowerCase());


            URI uri = builder.build();
            HttpPut request = new HttpPut(uri);
            request.setHeader("Content-Type", "application/json");
            request.setHeader("Ocp-Apim-Subscription-Key", APIkey);


            // Request body
            StringEntity reqEntity = new StringEntity("{\"name\":\"sample_list\",\"userData\":\"User-provided data attached to the face list\"}");
            request.setEntity(reqEntity);

            HttpResponse response = httpclient.execute(request);
            HttpEntity entity = response.getEntity();

            if (entity != null) 
            {
                System.out.println(EntityUtils.toString(entity));
            }
            else{
            	 System.out.println("Create FaceList successfully");
            	}
            
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }
     
     public static void AddToFaceList(String userID, String imageURL){
    	 String entityString=DetectFaces(imageURL,"false");
    	 if (entityString=="null"){System.out.println("no face detected, please upload a selfie");return;}
    	 System.out.println(entityString);
    	 Type listType = new TypeToken<List<JsonObject>>() {}.getType();
    	 List<JsonObject> faces = new Gson().fromJson(entityString, listType);
    	 if (faces.size()!=1){System.out.println("please upload a photo of yourself");return;}
    	Type type = new TypeToken<Map<String, String>>(){}.getType();
    	
    	 Map<String,String> rectangle=new Gson().fromJson(faces.get(0).get("faceRectangle").toString(), type);

    	 HttpClient httpclient = HttpClients.createDefault();
         
        try
         {
             URIBuilder builder = new URIBuilder("https://api.projectoxford.ai/face/v1.0/facelists/"+userID.toLowerCase()+"/persistedFaces");

             builder.setParameter("userData", "{string}");
             builder.setParameter("targetFace", rectangle.get("left").toString()+","+rectangle.get("top").toString()+","+rectangle.get("width").toString()+","+rectangle.get("height").toString());

             URI uri = builder.build();
             HttpPost request = new HttpPost(uri);
             request.setHeader("Content-Type", "application/json");
             request.setHeader("Ocp-Apim-Subscription-Key", APIkey);


             StringEntity reqEntity = new StringEntity("{\"url\":\""+imageURL+"\"}");
             request.setEntity(reqEntity);

             HttpResponse response = httpclient.execute(request);
             HttpEntity entity = response.getEntity();

             if (entity != null) 
             {
                 System.out.println(EntityUtils.toString(entity));
             }
         }
         catch (Exception e)
         {
             System.out.println(e.getMessage());
         }
     }
     
     
     
     public static boolean FindSimilar(String imageURL,String userID){
    	 String entityString=DetectFaces(imageURL,"true");
    	 if (entityString=="null")return false;
    	 Type listType = new TypeToken<List<JsonObject>>() {}.getType();
    	
    	 try{
    	 List<JsonObject> faces = new Gson().fromJson(entityString, listType);
    	 for (JsonObject face:faces){
        	 
        	 String faceId=face.get("faceId").toString();
        	 
        	HttpClient httpclient = HttpClients.createDefault();

             try
             {
                 URIBuilder builder = new URIBuilder("https://api.projectoxford.ai/face/v1.0/findsimilars");


                 URI uri = builder.build();
                 HttpPost request = new HttpPost(uri);
                 request.setHeader("Content-Type", "application/json");
                 request.setHeader("Ocp-Apim-Subscription-Key", APIkey);


                StringEntity reqEntity = new StringEntity("{\"faceId\":"+faceId+",\"faceListId\":\""+userID.toLowerCase()+"\",\"maxNumOfCandidatesReturned\":1,\"mode\":\"matchPerson\"}");
                 request.setEntity(reqEntity);

                 HttpResponse response = httpclient.execute(request);
                 HttpEntity entity = response.getEntity();
                 String result=EntityUtils.toString(entity);
                 if (result =="[]") 
                 {
                    continue;
                 }
                 if(!result.contains("confidence")){
                	 System.out.println(result);
                	 continue;
                 }
                 else{
                	 return true;
                 }
                 
             }
             catch (Exception e)
             {
                 System.out.println(e.getMessage());
             }
        	}
    	 }
    	 catch(Exception e){
    		System.out.println(e.getMessage());
    		 }
    	
    	 
    	 return false;
     }
     public static String DetectFaces(String imageURL, String returnFaceID){
         HttpClient httpclient = HttpClients.createDefault();

         try
         {
             URIBuilder builder = new URIBuilder("https://api.projectoxford.ai/face/v1.0/detect");

             builder.setParameter("returnFaceId", returnFaceID);
             builder.setParameter("returnFaceLandmarks", "false");

             URI uri = builder.build();
             HttpPost request = new HttpPost(uri);
             request.setHeader("Content-Type", "application/json");
             request.setHeader("Ocp-Apim-Subscription-Key", APIkey);

             StringEntity reqEntity = new StringEntity("{\"url\":\""+imageURL+"\"}");
             request.setEntity(reqEntity);

             HttpResponse response = httpclient.execute(request);
             HttpEntity entity = response.getEntity();

             if (entity != null) 
             {
                 return EntityUtils.toString(entity);
             }
             else return "null";
         }
         catch (Exception e)
         {
             return e.getMessage();
         }
     }
     
 }
