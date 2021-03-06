import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mortbay.util.ajax.JSON;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class LocalApp {
    // Fields
    private List<String> inputPaths;
    private List<String> outputPaths;
    private int n;
    private boolean terminate;
    private String name;
    private String bucketName;
    private String queueName;
    private Ec2Client ec2;
    private S3Client s3;
    private SqsClient sqs;
    private String local2managerURL;
    private String manager2localURL;
    private Boolean[] received;
    private JSONParser parser;
    private String managerID;

    // Constructor
    public LocalApp(List<String> inputPaths, List<String> outputPaths, int n, boolean terminate){
        this.inputPaths = inputPaths;
        this.outputPaths = outputPaths;
        this.n = n;
        this.terminate = terminate;
        this.name = "myapp" + new Date().getTime();
        this.bucketName = "bucket-" +name;
        this.queueName = "manager2local-" +name;
        this.ec2 = Ec2Client.create();
        this.s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        this.sqs = SqsClient.create();
        this.manager2localURL = createQueue();
        this.received = new Boolean[inputPaths.size()];
        Arrays.fill(received, false);
        this.parser = new JSONParser();
    }

    // Methods
    public void runApp(){

        managerID = getManager();
        System.out.println("Connected successfully to manager " +managerID);

        this.local2managerURL = getLocal2managerQueue();

        uploadFilesToS3();
        sendURLMessages2Manager();

        while(!isDone()) {
            String msg = readMessageFromManager();
            parseMessageFromManager(msg);
        }

        tearDown();
    }


    /*
    Gets the manager EC2 instance, creates one if necessary
     */
    private String getManager(){
        boolean done = false;
        String nextToken = null;

        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations())
                    for (Instance instance : reservation.instances())
                        for (Tag tag : instance.tags())
                            if(tag.value().equals("manager") &&
                                    (instance.state().name().toString().equals("pending") || instance.state().name().toString().equals("running")))
                                return instance.instanceId();

                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return createManager();
    }

    private String createManager() {

        Tag tag = Tag.builder()
                .key("name")
                .value("manager")
                .build();

        String userData = "#!/bin/bash" + "\n"
                + "wget https://ass1yonatanziv.s3.amazonaws.com/ManagerApp.jar" + "\n"
                + "java -jar ManagerApp.jar" + "\n";
        String base64UserData = null;

        try {
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(Utils.instanceType)
                .imageId(Utils.amiID)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("yz-role").build())
                .keyName("yonatan_ziv_key")
                .securityGroupIds("sg-07199d1ea166ce7fd")
//                .userData(Base64.getEncoder().encodeToString(base64UserData.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();


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

        System.out.println("Created successfully manager " +instanceId);
        return instanceId;
    }

    private void uploadFilesToS3(){

        CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                .bucket(this.bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build();

        s3.createBucket(bucketRequest);


        for(int i=0 ; i <inputPaths.size() ; i++){
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key("input" +i)
                    .acl(String.valueOf(BucketCannedACL.PUBLIC_READ))
                    .build();


            PutObjectResponse response = s3.putObject(objectRequest, Paths.get(inputPaths.get(i)));
        }

        System.out.println("Uploaded the input files to S3 successfully to bucket " +this.bucketName);
    }

    private String createQueue(){
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqs.createQueue(createQueueRequest);

        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());

        return getQueueUrlResponse.queueUrl();
    }

    private String getLocal2managerQueue(){
        String prefix = "local2manager", output = "";

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);

            while(listQueuesResponse.queueUrls().size() == 0) listQueuesResponse = sqs.listQueues(listQueuesRequest);

            output = listQueuesResponse.queueUrls().get(0);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return output;
    }

    /*
    Message format:
        * bucket  - bucket name
        * manager2local - the URL of the incoming messages queue
        * n - number of maximal tasks per worker
        * terminate - boolean that states whether to terminate Manager after this job
        * keys - array of input file keys in S3
    */
    private void sendURLMessages2Manager(){
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            JSONObject mainJson = new JSONObject();
            JSONArray keys = new JSONArray();

            mainJson.put("bucket",bucketName);
            mainJson.put("manager2local", manager2localURL);
            mainJson.put("n", n);
            mainJson.put("terminate", (terminate ? "true" : "false"));

            for (int i=0; i<objects.size();i++) {
                S3Object myValue = objects.get(i);
                keys.add(myValue.key());
            }

            mainJson.put("keys", keys);

            System.out.println("Sending message to manager:\n" +mainJson.toJSONString());

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(local2managerURL)
                    .messageBody(mainJson.toJSONString())
                    .build());

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private void parseMessageFromManager(String msg){
        JSONObject jsonMsg = null;
        try {
            jsonMsg = (JSONObject) parser.parse(msg);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String summaryName = jsonMsg.get("summeryKey").toString();
        int index = Integer.parseInt(jsonMsg.get("index").toString());

//        check if we didn't receive this message before
        if(received[index]) return;

        received[index] = true;


        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(summaryName)
                .build();

        ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
        BufferedReader reader = new BufferedReader(new InputStreamReader(response));

        StringBuilder output = new StringBuilder();

        try {
            String line;
            while ((line = reader.readLine()) != null)
                output.append(line);

        } catch (IOException e) {
            e.printStackTrace();
        }

        createHTML(output.toString(), outputPaths.get(index));
    }

    private void createHTML(String output, String outputFileName) {
        JSONObject parsedOutput = null;

        try {
            parsedOutput = (JSONObject) parser.parse(output);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        JSONArray summary = (JSONArray) parsedOutput.get("summary");

        File file = new File(outputFileName);
        FileWriter myWriter = null;
        try {
            myWriter = new FileWriter(outputFileName);


            myWriter.write("<html>\n" +
                    "<body>\n" +
                    "<ol>\n");

            for(Object rev : summary){
                String link = ((JSONObject)rev).get("link").toString();
                int sentiment = Integer.parseInt(((JSONObject)rev).get("sentiment").toString());
                String sarcasm = ((JSONObject)rev).get("sarcasm").toString();
                JSONArray entities = (JSONArray) ((JSONObject)rev).get("entities");


                myWriter.write("<li>\n" + "<ul>");

                // link
                myWriter.write("<li><a style=\"" +Utils.colors[sentiment] +"\" href=\"" +link +"\">" +link +"</a></li>\n");

                // sarcasm
                myWriter.write("<li>Sarcasm: " +sarcasm +"</li>");

                // entities
                myWriter.write("<li>Entities:<ul>\n");

                for(Object ent : entities){
                    myWriter.write("<li>" +ent.toString() +"</li>");
                }
                myWriter.write("</ul></li>\n");


                myWriter.write("</ul>\n" + "</li>");
            }

            myWriter.write("</ol>\n" + "</body>\n" + "</html>");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String readMessageFromManager() {
        String output = null;

        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(manager2localURL)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            while (messages.size() == 0) messages = sqs.receiveMessage(receiveMessageRequest).messages();

            output = messages.get(0).body();

            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(manager2localURL)
                    .receiptHandle(messages.get(0).receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return output;
    }

    private boolean isDone(){

        for(boolean b : received)
            if(!b) return false;

        return true;
    }

    private void tearDown(){
        if(terminate) {
            deleteManager();
            System.out.println("Manager Deleted successfully");
        }

        deleteBucket();
        System.out.println("Bucket Deleted successfully");

        deleteQueue();
        System.out.println("Queue Deleted successfully");
    }

    // Wait for OK termination message from the manager, and terminate Manager
    private void deleteManager() {
        String msg = readMessageFromManager();

        while(!msg.equals("terminate")) readMessageFromManager();

        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest
                .builder()
                .instanceIds(managerID)
                .build();

        ec2.terminateInstances(terminateRequest);

        System.out.println("Terminated manager with id: " +managerID);
    }

    private void deleteBucket(){
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(this.bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (S3Object myValue : objects) {
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                        .bucket(this.bucketName)
                        .key(myValue.key())
                        .build();

                s3.deleteObject(deleteObjectRequest);
            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(this.bucketName)
                .build();

        s3.deleteBucket(deleteBucketRequest);
    }

    private void deleteQueue(){
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
