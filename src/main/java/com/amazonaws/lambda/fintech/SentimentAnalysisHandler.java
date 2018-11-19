package com.amazonaws.lambda.fintech;

import java.io.File;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;
import com.amazonaws.services.comprehend.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectEntitiesResult;
import com.amazonaws.services.comprehend.model.DetectKeyPhrasesRequest;
import com.amazonaws.services.comprehend.model.DetectKeyPhrasesResult;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SentimentAnalysisHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    private AmazonComprehend comprehendClient =  AmazonComprehendClientBuilder.standard().build(); 

    public SentimentAnalysisHandler() {}

    // Test purpose only.
    SentimentAnalysisHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);

        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        String outputBucket = "fintech-lambda-output";
        String outputKey = "output";
        try {
        	 System.out.println("Input Bucket Name->"+bucket);    

 	         System.out.println("Input key Name->"+key); 
 	         
            S3Object s3Object = s3.getObject(new GetObjectRequest(bucket, key));
            String contentType = s3Object.getObjectMetadata().getContentType();
            context.getLogger().log("CONTENT TYPE: " + contentType);
            InputStream objectData = s3Object.getObjectContent();
            
            String textToUpload = "";
 		   Scanner scanner = new Scanner(objectData);	//scanning data line by line
 	               while (scanner.hasNext()) {
         		         textToUpload += scanner.nextLine();
 	               }
 	               scanner.close();
 	          
 	              System.out.println("Output Bucket Name->"+outputBucket);    

 	 	         System.out.println("Output key Name->"+outputKey); 
 	 	      
 	         
			// Call detectSentiment API
			System.out.println("Calling DetectSentiment");
			DetectSentimentRequest detectSentimentRequest = new DetectSentimentRequest().withText(textToUpload)
					.withLanguageCode("en");
			DetectSentimentResult detectSentimentResult = comprehendClient.detectSentiment(detectSentimentRequest);
			System.out.println(detectSentimentResult);
			System.out.println("End of DetectSentiment\n");
			System.out.println("Done");
			
			
			String outputFileName = key.substring(0, key.lastIndexOf("."));
			String sentimentOutputFileKey = outputKey+"/Sentiment/"+outputFileName;
			System.out.println("sentimentOutputFileKey->"+sentimentOutputFileKey);
			//s3.putObject(new PutObjectRequest(outputBucket,sentimentOutputFileKey+".csv", convertToCSV(detectSentimentResult)));
			s3.putObject(new PutObjectRequest(outputBucket,sentimentOutputFileKey+"_json", convertToText(addRequestId(detectSentimentResult,outputFileName).toString())));

			 // Call detectEntities API
	        System.out.println("Calling DetectEntities");
	        DetectEntitiesRequest detectEntitiesRequest = new DetectEntitiesRequest().withText(textToUpload)
	                                                                                                .withLanguageCode("en");
	        DetectEntitiesResult detectEntitiesResult  = comprehendClient.detectEntities(detectEntitiesRequest);
	        System.out.println(detectEntitiesResult);
			System.out.println("End of DetectEntities\n");
			System.out.println("Done");
			
			String entityOutputFileKey = outputKey+"/Entities/"+outputFileName;
			System.out.println("entityOutputFileKey->"+entityOutputFileKey);
			//s3.putObject(new PutObjectRequest(outputBucket,entityOutputFileKey+".txt", convertToText(detectEntitiesResult.toString())));
			s3.putObject(new PutObjectRequest(outputBucket,entityOutputFileKey+"_json", convertToText(addRequestId(detectEntitiesResult,outputFileName).toString())));
			 // Call detectKeyphrase API
	        System.out.println("Calling KeyPhrases Api");
	        DetectKeyPhrasesRequest detectKeyPhrasesRequest = new DetectKeyPhrasesRequest().withText(textToUpload)
	                                                                                                .withLanguageCode("en");
	        DetectKeyPhrasesResult detectKeyPhrasesResult  = comprehendClient.detectKeyPhrases(detectKeyPhrasesRequest);
	        System.out.println(detectKeyPhrasesResult);
			System.out.println("End of DetectKeyPhrases\n");
			System.out.println("Done");
			
			
			String phrasesoutputFileKey = outputKey+"/KeyPhrase/"+outputFileName;
			System.out.println("phrasesoutputFileKey->"+phrasesoutputFileKey);
			//s3.putObject(new PutObjectRequest(outputBucket,phrasesoutputFileKey+".csv", convertToCSV(detectKeyPhrasesResult)));
			s3.putObject(new PutObjectRequest(outputBucket,phrasesoutputFileKey+"_json", convertToText(addRequestId(detectKeyPhrasesResult,outputFileName).toString())));
            return "Success";
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", key, bucket));
            throw e;
        }
    }
    
    private static File convertToCSV(Object json) {
   	 
        JSONObject output;
        try {
            output = new JSONObject(json);
 
 
            JSONArray docs = output.toJSONArray(output.names());//getJSONArray("");
 
            File file=new File("/tmp/JSONSEPERATOR_CSV.csv");
            String csv = CDL.toString(docs);
            FileUtils.writeStringToFile(file, csv);
            System.out.println("Data has been Sucessfully Writeen to AbsolutePath "+file.getAbsolutePath());
            System.out.println("Data has been Sucessfully Writeen to Path "+file.getPath());
            return file;
        } catch (Exception e) {
            e.printStackTrace();
        }     
        return null;
   }
    
    private File convertToText(String json) {
        try {
            File file=new File("/tmp/JSONSEPERATOR_TXT.txt");
            FileUtils.writeStringToFile(file, json);
            System.out.println("Data has been Sucessfully Writeen to AbsolutePath "+file.getAbsolutePath());
            System.out.println("Data has been Sucessfully Writeen to Path "+file.getPath());
            return file;
        } catch (Exception e) {
            e.printStackTrace();
        }
		return null;      
   }
    
   private File convertToJson(Object object) {
	   ObjectMapper mapper = new ObjectMapper();

		/**
		 * Write object to file
		 */
		try {
			File output=new File("/tmp/result.json");
			mapper.writeValue(output, object);//Plain JSON
			return output;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
   }
   private static Object addRequestId(Object json,String outputFileName) {
	   	 
	    	JSONObject  output = new JSONObject(json);
	        Object str=(Object)outputFileName;
	       
	        try {
	    		output.put("ReviewRequestID", str);
	    		
	    	} catch (JSONException e1) {
	    		// TODO Auto-generated catch block
	    		e1.printStackTrace();
	    	}
	       Object obj=output;
	        System.out.println("output--->"+obj);
	        return obj;
	   }
   
}