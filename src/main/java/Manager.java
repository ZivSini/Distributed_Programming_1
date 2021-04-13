import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Manager {
    private Ec2Client ec2;
    private S3Client s3;
    private SqsClient sqs;
    private String local2manager;
    private String workers2manager;
    private String manager2workers;
    private HashMap<String, Job> jobs;

    public Manager(){
        this.ec2 = Ec2Client.create();
        this.s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        this.sqs = SqsClient.create();
        this.local2manager = createQueue("local2manager");
        this.workers2manager = createQueue("workers2manager");
        this.manager2workers = createQueue("manager2workers");
        this.jobs = new HashMap<>();
    }

    private String createQueue(String queueName){
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqs.createQueue(createQueueRequest);

        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());

        return getQueueUrlResponse.queueUrl();
    }

    private Job readMessageFromLocalApps(){
        Job output = null;

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2manager)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            output = new Job(messages.get(0).body() , s3);
    } catch (SqsException e) {
        System.err.println(e.awsErrorDetails().errorMessage());
        System.exit(1);
    }
        return output;
    }

    private void readMessageFromWorkers(){

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2manager)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            String[] msg = messages.get(0).body().split(" ");

            Job job = jobs.get(msg[0]);
            int index = Integer.parseInt(msg[1]);
            int sentiment = Integer.parseInt(msg[2]);
            List<String> entities = Arrays.asList(msg).subList(3, msg.length);

            job.addResult(entities, sentiment, index);
            if(job.isDone())
              summarizeAndSend2Local(job);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /*
    summarize all the job reviews into json object,
    uploads the json to s3,
    sends the local app message in s3: <key of summary in localApp s3 bucket>
     */
    private void summarizeAndSend2Local(Job job) {
        JSONObject mainJson = new JSONObject();
        mainJson.put("key", job.getObjectKey());

        JSONArray json = new JSONArray();
        for(Review review: job.getReviews()){
            JSONObject obj = new JSONObject();
            obj.put("link",review.getLink());
            obj.put("sentiment",review.getSentiment());
            JSONArray entities = new JSONArray();
            for (String e:review.getEntities()) {
                entities.add(e);
            }
            obj.put("entities", entities);
            obj.put("sarcasm",Math.abs(review.getRating() - review.getSentiment()));
            json.add(obj);
        }

        mainJson.put("summary" ,json);
        try (FileWriter file = new FileWriter("summary_"+job.getObjectKey() +".json")) {
            //We can write any JSONArray or JSONObject instance to the file
            file.write(mainJson.toJSONString());
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(job.getBucketName())
                .key("summary_" +job.getObjectKey())
                .acl(String.valueOf(BucketCannedACL.PUBLIC_READ))
                .build();

        PutObjectResponse response = s3.putObject(objectRequest, Paths.get("summary_"+job.getObjectKey() +".json"));

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(job.getManager2local())
                .messageBody("summary_"+job.getObjectKey() + ".json")
                .build());
    }

    /*
    Sends tasks from job to workers queue.
    message format: <bucket/key> <index>
     */
    private void sendTasksToWorkers(Job job){
        for(int i=0 ; i<job.getReviews().size() ; i++){
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(manager2workers)
                    .messageBody(job.getBucketName() +"/" +job.getObjectKey() +" " +i)
                    .build());
        }
    }

    public String getLocal2manager() {
        return local2manager;
    }

}
