import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

public class Main {
    public static void main(String[] args) {


        // TODO: delete
        teardown();

        List<String> inputFiles = new LinkedList<>();
        List<String> outputFiles = new LinkedList();

        boolean terminate = args[args.length -1].equals("terminate");
        int off = (terminate ? 1 : 0);
        int numOfFiles = (args.length -1 -off) /2;
        int n = Integer.parseInt(args[args.length -1 -off]);

        for(int i=0 ; i<numOfFiles ; i++){
            inputFiles.add(args[i]);
            outputFiles.add(args[numOfFiles +i]);
        }

        LocalApp app = new LocalApp(inputFiles, outputFiles, n, terminate);
        app.runApp();

////    Worker worker = new Worker();




//
//        l1.add("inputs/0689835604.txt");
//        l1.add("inputs/B000EVOSE4.txt");
//        l1.add("inputs/B01LYRCIPG.txt");
//        l1.add("inputs/B001DZTJRQ.txt");
//        l1.add("inputs/B0047E0EII.txt");
//
////
//        LocalApp app = new LocalApp(l1, l2, 1);
//        app.runApp();
//        Manager man = new Manager();
//        SqsClient sqs = SqsClient.create();
//        sqs.sendMessage(SendMessageRequest.builder()
//                .queueUrl(man.getLocal2manager())
//                .messageBody("bucketName" +" " +"myValue.key()" +" " +"manager2localURL")
//                .build());
//
//        Job job = man.readMessageFromLocalApps();
//
//        System.out.println(job.reviews);

    }


    public static void teardown()
    {
        deleteBuckets();
        deleteQueues();
        deleteEC2();
    }

    public static void deleteQueues()
    {
        String prefix = "", output = "";
        SqsClient sqs = SqsClient.create();

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);

            if (listQueuesResponse.queueUrls().size() > 0)
            {
                for (String qURL: listQueuesResponse.queueUrls())
                {
                    DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                            .queueUrl(qURL)
                            .build();

                    sqs.deleteQueue(deleteQueueRequest);
                }
            }
        }catch (Exception e){}
    }

        public static void deleteBuckets(){
            S3Client s3 = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .build();

            ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
            ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
            listBucketsResponse.buckets().stream().forEach(x ->
            { if(!x.name().equals("ass1yonatanziv")) deleteBucket(s3, x.name()); }
            );


        }

        private static void deleteBucket(S3Client s3, String bucket){

            try {
                // To delete a bucket, all the objects in the bucket must be deleted first
                ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
                ListObjectsV2Response listObjectsV2Response;

                do {
                    listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                    for (S3Object s3Object : listObjectsV2Response.contents()) {
                        s3.deleteObject(DeleteObjectRequest.builder()
                                .bucket(bucket)
                                .key(s3Object.key())
                                .build());
                    }

                    listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket)
                            .continuationToken(listObjectsV2Response.nextContinuationToken())
                            .build();

                } while(listObjectsV2Response.isTruncated());
                // snippet-end:[s3.java2.s3_bucket_ops.delete_bucketobjects]

                DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
                s3.deleteBucket(deleteBucketRequest);

            } catch (S3Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }

        public static void deleteEC2(){
            List<String> instancesIDs = new ArrayList<>();
            String nextToken="";
            Ec2Client ec2 = Ec2Client.create();

            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations())
                    for (Instance instance : reservation.instances())
                        if((instance.state().name().toString().equals("pending") || instance.state().name().toString().equals("running")))
                            instancesIDs.add(instance.instanceId());

                nextToken = response.nextToken();
            } while (nextToken != null);

            if(!instancesIDs.isEmpty()) {
                TerminateInstancesRequest terminateRequest = TerminateInstancesRequest
                        .builder()
                        .instanceIds(instancesIDs)
                        .build();

                ec2.terminateInstances(terminateRequest);
            }
        }
    }
