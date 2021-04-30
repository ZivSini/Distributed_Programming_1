import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Job {
    private String bucketName;
    private String objectKey;
    private String manager2local;
    private ArrayList<Review> reviews;
    private JSONParser parser;
    private int n;
    private boolean terminate;
    private int numJobs;
    private int jobIndex;


    /*
    Constructor. Takes the msg from the local app, and parses it.
    Message format: <bucket> <key> <manager2local> <n>
     */
    public Job(String bucketName, String manager2local, int n, boolean terminate, String objectKey, S3Client s3 ){

//        JSONObject jsonMsg = null;
//
//        this.parser = new JSONParser();
//        try {
//            jsonMsg = (JSONObject) parser.parse(msg);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }

        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.manager2local = manager2local;
        this.n = n;
        this.terminate = terminate;

        System.out.println("Job created!");
        System.out.println("bucketName: " + bucketName);
        System.out.println("objectKeys: " + objectKey);
        System.out.println("manager2local: " + manager2local);
        System.out.println("n: " + n);
        System.out.println("terminate: " + terminate);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
        BufferedReader reader = new BufferedReader(new InputStreamReader(response));

        String line;
        this.reviews = new ArrayList<>();
        JSONParser parser = new JSONParser();

        try {
            while ((line = reader.readLine()) != null) {
                JSONObject json = (JSONObject) parser.parse(line);
                JSONArray reviewsJson = (JSONArray) json.get("reviews");
                for(int i=0 ; i<reviewsJson.size() ; i++){
                    JSONObject jsonReview = (JSONObject) reviewsJson.get(i);
                    String text = jsonReview.get("text").toString();
                    String link = jsonReview.get("link").toString();
                    int rating = Integer.parseInt(jsonReview.get("rating").toString());

                    reviews.add(new Review(text, link, rating, i));
                }
            }
        } catch (Exception ignored) {}
    }

    public static List<Job> getJobs(String msg, S3Client s3){
        JSONObject jsonMsg = null;
        List<Job> output = new ArrayList<>();

        JSONParser parser = new JSONParser();
        try {
            jsonMsg = (JSONObject) parser.parse(msg);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Creating Job from msg:\n" +msg);

        String bucket = jsonMsg.get("bucket").toString();
        String manager2local = jsonMsg.get("manager2local").toString();
        int n = Integer.parseInt(jsonMsg.get("n").toString());
        boolean terminate = jsonMsg.get("terminate").toString().equals("true");

        JSONArray keys = (JSONArray) jsonMsg.get("keys");

        for(Object key : keys)
            output.add(new Job(bucket, manager2local, n, terminate, key.toString(), s3));

        return output;
    }

    /*
    Updates the ith review with the result.
    Returns true iff the job is now done.
     */
    public void addResult(List<String> entities, int sentiment, int i){
        Review review = reviews.get(i);

        if(!review.isDone())
            review.setResult(entities, sentiment);

    }

    public List<Review> getReviews() {
        return reviews;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public void setObjectKey(String objectKey) {
        this.objectKey = objectKey;
    }

    public boolean isDone(){
        boolean output = true;
        for(Review rev : reviews)
            if(!rev.isDone()) return false;

        return true;
        // TODO: clean

        //        return reviews.size() == resultCounter;
    }

    public String getManager2local() {
        return manager2local;
    }

    public int getN() {
        return n;
    }

    public int getWorkersN(){
        System.out.println("reviews.size() = " +reviews.size() +", n = " +n);
        return reviews.size() / n;
    }

    public String getBucketKey(){
        return  bucketName +"/" +objectKey;
    }

    public boolean isTerminate() {
        return terminate;
    }

    public int getNumJobs() {
        return numJobs;
    }

    public int getJobIndex() {
        return jobIndex;
    }
}
