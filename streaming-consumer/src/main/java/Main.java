import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.bson.Document;

import java.util.*;

public class Main {
    private static String Topic = "test";

    public static void main(String[] args) throws Exception {

        System.out.println("RUNNING STREAMING-CONSUMER");
        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME");
        if (initialSleepTime != null && !initialSleepTime.equals("")) {
            int sleep = Integer.parseInt(initialSleepTime);
            System.out.println("Sleeping on start " + sleep + "sec");
            Thread.sleep(sleep * 1000);
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }

        String mongoUrl = System.getenv("MONGO_URL");
        if (mongoUrl == null || mongoUrl.equals("")) {
            throw new IllegalStateException("MONGO_URL environment variable must be set");
        }

        SparkConf conf = new SparkConf().setAppName("BigData-2").setMaster(sparkMasterUrl);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(5000));

        System.out.println("Created streaming context");

        Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
        Collection<String> topics = Collections.singletonList(Topic);

        JavaInputDStream<ConsumerRecord<Object, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);

        receivedData.foreachRDD(d -> {
            d.foreach(a -> {
                EventData tmp = EventData.CreateEventData(a);
                if (tmp != null) {
                    MongoClientURI connectionString = new MongoClientURI(mongoUrl);
                    MongoClient mongoClient = new MongoClient(connectionString);
                    MongoDatabase database = mongoClient.getDatabase("database");
                    MongoCollection<Document> collection = database.getCollection("test");
                    Document doc = new Document("data", a);
                    collection.insertOne(doc);
                    mongoClient.close();
                }

            });
        });

        receivedData.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Map<String, Object> getKafkaParams(String brokers) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return kafkaParams;
    }
}
