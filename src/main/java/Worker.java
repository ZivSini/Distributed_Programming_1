//import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.util.Pair;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;

import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class Worker {
    private String manager2workers;
    private String workers2manager;
    private Ec2Client ec2;
    private S3Client s3;
    private SqsClient sqs;
    private String bucketKey;
    private String index;
    private String text;
    private Properties entitiesProps;
    private Properties sentimentProps;
    private StanfordCoreNLP entitiesNERPipeline;
    private StanfordCoreNLP sentimentNERPipeline;
    private JSONParser parser;

//    private SentimentAnalysisHandler sentimentAnalysisHandler;
//    private namedEntityRecognitionHandler namedEntityRecognitionHandler;

    public static void main(String[] args) {
        System.out.println("Started Worker main");
        Worker worker = new Worker();
        worker.runWorker();
    }

    private void runWorker() {
        while(true){
            Message message = readMessage(); // loads bucketKey, index, text to fields
            System.out.println("worker received message: "+message.body());

            int sentiment = findSentiment(text);
            System.out.println("sentiment: " + sentiment);

            List<String> entities = findEntities(text);
            System.out.println("entities: " + entities);

            sendMessage2Manager(sentiment, entities);
            System.out.println("sending message to manager");

            deleteMessage(message);
            System.out.println("deleted message");
        }
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(manager2workers)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    public Worker() {
        this.ec2 = Ec2Client.create();
        this.s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        this.sqs = SqsClient.create();

        parser = new JSONParser();

        this.manager2workers = getQueue("manager2workers");
        this.workers2manager = getQueue("workers2manager");

        entitiesProps = new Properties();
        entitiesProps.put("annotators", "tokenize, ssplit, pos, lemma, ner");

        // TODO: fix bug
//        sentimentProps = new Properties();
//        sentimentProps.put("annotators", "tokenize, ssplit, parse, sentiment");

        entitiesNERPipeline = new StanfordCoreNLP(entitiesProps);
//        sentimentNERPipeline = new StanfordCoreNLP(sentimentProps);

//        System.out.println(findSentiment("The banana is very useful"));
//        printEntities("the banana is very useful");
    }

    private String getQueue(String prefix){
        String output = "";

        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);

            // busy wait to get the queue
            while (listQueuesResponse.queueUrls().size() == 0) listQueuesResponse = sqs.listQueues(listQueuesRequest);
            output = listQueuesResponse.queueUrls().get(0);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return output;
    }

    /*
    Receives message from manager.
    Message format: <bucket/key> <index> <reviewText>
    TODO: deal with terminate msg
     */
    private Message readMessage(){
        try{
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(manager2workers)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();

            //busy wait to get message from sqs
            while (messages.size() == 0) messages = sqs.receiveMessage(receiveMessageRequest).messages();

            String msg = messages.get(0).body();

            JSONObject jsonMsg = (JSONObject) parser.parse(msg);
            this.bucketKey = jsonMsg.get("bucketKey").toString();
            this.index = jsonMsg.get("index").toString();
            this.text = jsonMsg.get("reviewText").toString();

            return messages.get(0);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int findSentiment(String review) {
//        int mainSentiment = 0;
//        if (review!= null && review.length() > 0) {
//            int longest = 0;
//            Annotation annotation = sentimentNERPipeline.process(review);
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
        // TODO: fix bug
        return 3;
    }

    private List<String> findEntities(String review){
        List<String> output = new LinkedList<>();
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
        // run all Annotators on this text
        entitiesNERPipeline.annotate(document);
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with
        //        custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
        // traversing the words in the current sentence
        // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
//                String ne = token.get(NamedEntityTagAnnotation.class);

                output.add(word);
            }
        }

        return output;
    }

    private void sendMessage2Manager(int sentiment, List<String> entities){
        JSONObject result = new JSONObject();
        result.put("bucketKey", bucketKey);
        result.put("index", index);
        result.put("sentiment", sentiment);

        JSONArray entitiesJSON = new JSONArray();
        for(String ent : entities) entitiesJSON.add(ent);

        result.put("entities", entitiesJSON);

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(workers2manager)
                .messageBody(result.toJSONString())
                .build());
    }

}
