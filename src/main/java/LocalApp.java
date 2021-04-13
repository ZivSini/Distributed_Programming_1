import org.json.simple.JSONObject;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

public class LocalApp {
    // Fields
    private List<String> inputPaths;
    private List<String> outputPaths;
    private int n;
    private String name;
    private String bucketName;
    private String queueName;
    private Ec2Client ec2;
    private S3Client s3;
    private SqsClient sqs;
    private String local2managerURL;
    private String manager2localURL;

    // Constructor
    public LocalApp(List<String> inputPaths, List<String> outputPaths, int n){
        this.inputPaths = inputPaths;
        this.outputPaths = outputPaths;
        this.n = n;
        this.name = "myapp" + new Date().getTime();
        this.bucketName = "bucket-" +name;
        this.queueName = "manager2local-" +name;
        this.ec2 = Ec2Client.create();
        this.s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        this.sqs = SqsClient.create();
        this.manager2localURL = createQueue();

    }

    // Methods
    public void runApp(){
      System.out.println(getManager());
      this.local2managerURL = getLocal2managerQueue();

      System.out.println(uploadFilesToS3());

      sendURLMessages2Manager();
//      deleteBucket();
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

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(Utils.amiID)
                .maxCount(1)
                .minCount(1)
                .keyName("yonatan_ziv_key")

//                .userData(Base64.getEncoder().encodeToString(/*your USER DATA script string*/.getBytes()))
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
                    "Successfully started EC2 Instance %s based on AMI %s",
                    instanceId, Utils.amiID);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return instanceId;
    }

    private String uploadFilesToS3(){

        String bucketName = this.bucketName;

        CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
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


        return bucketName;
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



        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(this.bucketName).build();
        s3.deleteBucket(deleteBucketRequest);

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

    private String getLocal2managerQueue(){
        String prefix = "local2manager", output = "";

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);

            output = listQueuesResponse.queueUrls().get(0);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return output;
    }

    private void sendURLMessages2Manager(){
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (S3Object myValue : objects) {
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(local2managerURL)
                        .messageBody(bucketName +" " +myValue.key() +" " +manager2localURL)
                        .build());
            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }



    }


    private void parseMessageFromManager(JSONObject json){

    }
}
