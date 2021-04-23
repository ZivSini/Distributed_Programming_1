import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        List<String> inputFiles = new LinkedList<String>();
        List<String> outputFiles = new LinkedList<String>();

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

//    Worker worker = new Worker();




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
}
