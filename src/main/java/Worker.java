////import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
//import edu.stanford.nlp.rnn.RNNCoreAnnotations;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.ec2.Ec2Client;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.sqs.SqsClient;
//import software.amazon.awssdk.services.sqs.model.*;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;
//
//import java.util.List;
//import java.util.Properties;
//import edu.stanford.nlp.ling.CoreAnnotations;
//import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
//import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
//import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
//import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
//import edu.stanford.nlp.ling.CoreLabel;
//import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import edu.stanford.nlp.trees.Tree;
//import edu.stanford.nlp.util.CoreMap;
//
//public class Worker {
//    private String manager2workers;
//    private String workers2manager;
//    private Ec2Client ec2;
//    private S3Client s3;
//    private SqsClient sqs;
//    private String bucketKey;
//    private String index;
//    private String text;
//    private Properties props;
//    private StanfordCoreNLP NERPipeline;
//
//    private sentimentAnalysisHandler sentimentAnalysisHandler;
//    private namedEntityRecognitionHandler namedEntityRecognitionHandler;
//
//    public Worker() {
////        this.ec2 = Ec2Client.create();
////        this.s3 = S3Client.builder()
////                .region(Region.US_EAST_1)
////                .build();
////        this.sqs = SqsClient.create();
////
////        this.manager2workers = getQueue("manager2workers");
////        this.workers2manager = getQueue("workers2manager");
//
////
//        this.props = new Properties();
//        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
//         this.NERPipeline = new StanfordCoreNLP(props);
//        this.sentimentAnalysisHandler = new sentimentAnalysisHandler();
//        this.namedEntityRecognitionHandler = new namedEntityRecognitionHandler();
//
//
////        Properties props = new StanfordCoreNLP(props);
//
//        System.out.println(findSentiment("what is up, my dearest nigger?"));
////        printEntities("what is up, my dearest nigger?");
//    }
//
//    private String getQueue(String prefix){
//        String output = "";
//
//        try {
//            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
//            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
//
//            output = listQueuesResponse.queueUrls().get(0);
//
//        } catch (SqsException e) {
//            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(1);
//        }
//
//        return output;
//    }
//
//    /*
//    Receives message from manager.
//    Message format: <bucket/key> <index> <review text>
//    TODO: deal with terminate msg
//     */
//    private void readMessage(){
//        try{
//            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
//                    .queueUrl(manager2workers)
//                    .maxNumberOfMessages(1)
//                    .build();
//            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
//            String[] msg = messages.get(0).body().split(" ");
//            this.bucketKey = msg[0];
//            this.index = msg[1];
//            this.text = String.join(" ", Arrays.asList(msg).subList(2, msg.length));
//
//        } catch (SqsException e) {
//            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(1);
//        }
//    }
//
//    private int findSentiment(String review) {
//        int mainSentiment = 0;
//        if (review!= null && review.length() > 0) {
//            int longest = 0;
//            Annotation annotation = NERPipeline.process(review);
//            for (CoreMap sentence : annotation
//                    .get(CoreAnnotations.SentencesAnnotation.class)) {
//                Tree tree = sentence.get(
//                        SentimentCoreAnnotations.AnnotatedTree.class);
//                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
//                String partText = sentence.toString();
//                if (partText.length() > longest) {
//                    mainSentiment = sentiment;longest = partText.length();
//                }
//            }
//        }
//        return mainSentiment;
//    }
//
////    public void printEntities(String review){
////        // create an empty Annotation just with the given text
////        Annotation document = new Annotation(review);
////        // run all Annotators on this text
////        NERPipeline.annotate(document);
////        // these are all the sentences in this document
////        // a CoreMap is essentially a Map that uses class objects as keys and has values with
////        //        custom types
////        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
////        for(CoreMap sentence: sentences) {
////        // traversing the words in the current sentence
////        // a CoreLabel is a CoreMap with additional token-specific methods
////            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
////                // this is the text of the token
////                String word = token.get(TextAnnotation.class);
////                // this is the NER label of the token
////                String ne = token.get(NamedEntityTagAnnotation.class);
////                System.out.println("\t-" + word + ":" + ne);
////            }
////        }
////    }
//
//    public void printEntities(String review){
//// create an empty Annotation just with the given text
//        Annotation document = new Annotation(review);
//// run all Annotators on this text
//        NERPipeline.annotate(document);
//// these are all the sentences in this document
//// a CoreMap is essentially a Map that uses class objects as keys and has values with
////        custom types
//        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
//        for(CoreMap sentence: sentences) {
//// traversing the words in the current sentence
//// a CoreLabel is a CoreMap with additional token-specific methods
//            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
//// this is the text of the token
//                String word = token.get(TextAnnotation.class);
//// this is the NER label of the token
//                String ne = token.get(NamedEntityTagAnnotation.class);
//                System.out.println("\t-" + word + ":" + ne);
//            }
//        }
//    }
//}
