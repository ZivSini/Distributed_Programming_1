import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Manager {

    private static Ec2Client ec2 = Ec2Client.create();
    private static S3Client s3 = S3Client.builder()
            .region(Region.US_EAST_1)
            .build();
    private static SqsClient sqs = SqsClient.create();
    private static String local2manager = createQueue("local2manager");
    private static String workers2manager = createQueue("workers2manager");
    private static String manager2workers = createQueue("manager2workers");
    private static HashMap<String, Job> jobs = new HashMap<>();
    private static List<String> workers = new ArrayList<>();

    public static void main(String[] args){

        new Thread(() -> {
            while (true)
                readMessageFromWorkers();
        }).start();

        while(true){
            Job job = readMessageFromLocalApps();

            if(job.getWorkersN() > workers.size()){
                createKWorkers(job.getWorkersN() - workers.size());
            }

            sendTasksToWorkers(job);
            // TODO: should we kill workers before terminate?
        }

    }

    private static String createQueue(String queueName){
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqs.createQueue(createQueueRequest);

        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());

        return getQueueUrlResponse.queueUrl();
    }

    // Message format: <bucket> <key> <manager2local sqs URL> <n>
    private static Job readMessageFromLocalApps(){
        Job output = null;

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(local2manager)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            output = new Job(messages.get(0).body() , s3);
            // TODO: deal with number of workers (job.getN)
    } catch (SqsException e) {
        System.err.println(e.awsErrorDetails().errorMessage());
        System.exit(1);
    }
        return output;
    }

    private static void readMessageFromWorkers(){

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(workers2manager)
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
    sends the local app message in s3: <key of summary in localApp s3 bucket> <file number>
     */
    private static void summarizeAndSend2Local(Job job) {
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
        String num = job.getObjectKey().split("input")[1];

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(job.getManager2local())
                .messageBody("summary_"+job.getObjectKey() + ".json " +num)
                .build());
    }

    /*
    Sends tasks from job to workers queue.
    message format: <bucket/key> <index> <review text>
     */
    private static void sendTasksToWorkers(Job job){
        for(int i=0 ; i<job.getReviews().size() ; i++){
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(manager2workers)
                    .messageBody(job.getBucketName() +"/" +job.getObjectKey() +" " +i +" " +job.getReviews().get(i).getText())
                    .build());
        }
    }

    public static String getLocal2manager() {
        return local2manager;
    }

    /*
    Creates k new worker instances and updates the workers list
     */
    private static void createKWorkers(int k){

        Tag tag = Tag.builder()
                .key("name")
                .value("worker")
                .build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(Utils.amiID)
                .maxCount(k)
                .minCount(k)
                .keyName("yonatan_ziv_key")

//                .userData(Base64.getEncoder().encodeToString(/*your USER DATA script string*/.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        for(Instance instance : response.instances()) {
            String instanceId = instance.instanceId();
            workers.add(instanceId);


            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "Successfully started EC2 Instance %s based on AMI %s",
                        instanceId, Utils.amiID);
            } catch (Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
    }

    private static void tearDown(){
        deleteQueue("local2manager");
        deleteQueue("workers2manager");
        deleteQueue("manager2workers");
    }

    private static void deleteQueue(String queueName){
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
