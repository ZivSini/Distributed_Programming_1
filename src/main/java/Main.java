import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.*;

public class Main {
    public static void main(String[] args) {
//        List<String> l1 = new LinkedList<String>();
//        List<String> l2 = new LinkedList<String>();
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
