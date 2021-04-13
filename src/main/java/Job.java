import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
    private List<Review> reviews;
    private int resultCounter;


    /*
    Constructor. Takes the msg from the local app, and parses it.
    Message format: <bucket name> <object key> <manager2local SQS URL>
     */
    public Job(String msg, S3Client s3){
        String[] arr = msg.split(" ");

        this.resultCounter = 0;
        this.bucketName = arr[0];
        this.objectKey = arr[1];
        this.manager2local = arr[2];

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
                JSONArray reviews = (JSONArray) json.get("reviews");
                for(int i=0 ; i<reviews.size() ; i++){
                    JSONObject jsonReview = (JSONObject) reviews.get(i);
                    String text = jsonReview.get("text").toString();
                    String link = jsonReview.get("link").toString();
                    int rating = Integer.parseInt(jsonReview.get("rating").toString());

                    reviews.add(new Review(text, link, rating, i));
                }
            }
        } catch (Exception ignored) {}
    }

    /*
    Updates the ith review with the result.
    Returns true iff the job is now done.
     */
    public void addResult(List<String> entities, int sentiment, int i){
        Review review = reviews.get(i);

        if(!review.isDone()){
            review.setResult(entities, sentiment);
            resultCounter++;
        }

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
        return reviews.size() == resultCounter;
    }


    public String getManager2local() {
        return manager2local;
    }

}
