import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.*;

public class Manager {

    private Ec2Client ec2;
    private S3Client s3;
    private SqsClient sqs;
    private String local2manager;
    private String workers2manager;
    private String manager2workers;
    private HashMap<String, Job> jobs;
    private List<String> workers;
    private JSONParser parser;

    public static void main(String[] args){

        System.out.println("Running manager Main");
        Manager manager = new Manager();
        manager.runManager();
    }

    public Manager(){
        System.out.println("Creating new manager");

        ec2 = Ec2Client.create();
        s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        sqs = SqsClient.create();
        local2manager = createQueue("local2manager");
        workers2manager = createQueue("workers2manager");
        manager2workers = createQueue("manager2workers");
        jobs = new HashMap<>();
        workers = new ArrayList<>();
        parser = new JSONParser();

        System.out.println("Created manager successfully");
    }

    public void runManager(){
        new Thread(() -> {
            System.out.println("Listener thread created");
            while (true)
                readMessageFromWorkers();
        }).start();

        while(true){
            System.out.println("Listening to local apps");
            Job job = readMessageFromLocalApps();

            jobs.put(job.getBucketKey(), job);

            System.out.println("job.getWorkersN: "+ job.getWorkersN());
            System.out.println("workers.size(): "+ workers.size());
            if(job.getWorkersN() > workers.size()){
                System.out.println("entered if statement");
                createKWorkers(job.getWorkersN() - workers.size());
            }

            sendTasksToWorkers(job);
            // TODO: should we kill workers before terminate?
        }
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

    // Message format: <bucket> <key> <manager2local sqs URL> <n>
    private Job readMessageFromLocalApps(){
        Job output = null;

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2manager)
                    .maxNumberOfMessages(1)
                    .build();

            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();

            // Busy wait for new message
            while(messages.size() == 0) messages = sqs.receiveMessage(receiveMessageRequest).messages();

            System.out.println("Received a message from a local app:");
            System.out.println(messages.get(0).body());

            output = new Job(messages.get(0).body() , s3);

            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(local2manager)
                    .receiptHandle(messages.get(0).receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return output;
    }

    private void readMessageFromWorkers(){

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(workers2manager)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();

            // Busy wait for a new message
            while(messages.size() == 0) messages = sqs.receiveMessage(receiveMessageRequest).messages();

            System.out.println("New message received");

            JSONObject msg = (JSONObject) parser.parse(messages.get(0).body());

            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(workers2manager)
                    .receiptHandle(messages.get(0).receiptHandle())
                    .build();

            sqs.deleteMessage(deleteMessageRequest);

            System.out.println("Received message from workers:");
            System.out.println(msg.toJSONString());

            Job job = jobs.get(msg.get("bucketKey").toString());
            int index = Integer.parseInt(msg.get("index").toString());
            int sentiment = Integer.parseInt(msg.get("sentiment").toString());

            List<String> entities = new ArrayList();
            JSONArray jArray = (JSONArray) msg.get(0);

            if (jArray != null)
                for (int i=0 ; i<jArray.size() ; i++) entities.add(jArray.get(i).toString());

            job.addResult(entities, sentiment, index);

            if(job.isDone())
                System.out.println("Job is Done");
                summarizeAndSend2Local(job);

        } catch (SqsException | ParseException e) {
            System.err.println(e);
            System.exit(1);
        }
    }

    /*
    summarize all the job reviews into json object,
    uploads the json to s3,
    sends the local app message in s3: <key of summary in localApp s3 bucket> <file number>
     */
    private void summarizeAndSend2Local(Job job) {
        JSONObject mainJson = new JSONObject();
        mainJson.put("key", job.getObjectKey());

        JSONArray reviewsJSON = new JSONArray();

        for(Review review: job.getReviews()){
            JSONObject singleReviewJSON = new JSONObject();

            // Add review's properties
            singleReviewJSON.put("link",review.getLink());
            singleReviewJSON.put("sentiment",review.getSentiment());
            singleReviewJSON.put("sarcasm",Math.abs(review.getRating() - review.getSentiment()));

            // Add review's entities
            JSONArray entitiesJsonArr = new JSONArray();

            if(review == null) System.out.println("Review is null!");
            else if(review.getEntities() == null) System.out.println("Review's entities list is null!");
            else for(String s : review.getEntities()) System.out.println(s);

//            review.getEntities is null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            for(String s : review.getEntities()) {
                entitiesJsonArr.add(s);
            }

            singleReviewJSON.put("entities", entitiesJsonArr);

            reviewsJSON.add(singleReviewJSON);
        }

        mainJson.put("summary" ,reviewsJSON);
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
        String num = job.getObjectKey().split("input")[1];

        JSONObject jsonMsg = new JSONObject();
        jsonMsg.put("summeryKey", "summary_"+job.getObjectKey() + ".json");
        jsonMsg.put("index", num);

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(job.getManager2local())
                .messageBody(jsonMsg.toString())
                .build());

        System.out.println("Job is summarized and sent to local app");
    }

    /*
    Sends tasks from job to workers queue.
    message format: <bucket/key> <index> <reviewText>
     */
    private void sendTasksToWorkers(Job job){
        for(int i=0 ; i<job.getReviews().size() ; i++){
            JSONObject json = new JSONObject();

            json.put("bucketKey", job.getBucketKey());
            json.put("index", i);
            json.put("reviewText", job.getReviews().get(i).getText());


            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(manager2workers)
                    .messageBody(json.toJSONString())
                    .build());
        }
    }

    public  String getLocal2manager() {
        return local2manager;
    }

    /*
    Creates k new worker instances and updates the workers list
     */
    private void createKWorkers(int k){
        if(workers.size() < 18) {

            // Number of workers can't be greater then 18 (AWS account restrictions)
            int workersToCreate = k + workers.size() > 18 ? 18 -workers.size() : k;

            Tag tag = Tag.builder()
                    .key("name")
                    .value("worker")
                    .build();

            String userData = "#!/bin/bash" + "\n"
                    + "wget https://ass1yonatanziv.s3.amazonaws.com/WorkerApp.jar" + "\n"
                    + "java -jar WorkerApp.jar" + "\n";
            String base64UserData = null;

            try {
                base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }


            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(Utils.instanceType)
                    .imageId(Utils.amiID)
                    .maxCount(workersToCreate)
                    .minCount(workersToCreate)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("yz-role").build())
                    .keyName("yonatan_ziv_key")
                    .securityGroupIds("sg-07199d1ea166ce7fd")
//                    .userData(Base64.getEncoder().encodeToString(base64UserData.getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            // TODO: delete
            System.out.println("Created " +workersToCreate +" workers.");

            for (Instance instance : response.instances()) {
                String instanceId = instance.instanceId();
                workers.add(instanceId);

                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(instanceId)
                        .tags(tag)
                        .build();

                try {
                    ec2.createTags(tagRequest);
                    System.out.printf(
                            "Successfully started EC2 Instance %s based on AMI %s\n",
                            instanceId, Utils.amiID);
                } catch (Ec2Exception e) {
                    System.err.println(e.awsErrorDetails().errorMessage());
                    System.exit(1);
                }
            }
        }
    }

    private void tearDown(){
        deleteQueue("local2manager");
        deleteQueue("workers2manager");
        deleteQueue("manager2workers");
    }

    private void deleteQueue(String queueName){
        try {

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
