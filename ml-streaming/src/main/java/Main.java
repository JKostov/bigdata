import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final String Topic = "traffic-data";

    public static void main(String[] args) throws Exception {

        System.out.println("RUNNING ML-STREAMING");
        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
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
        String dataReceivingTimeInSeconds = System.getenv("DATA_RECEIVING_TIME_IN_SECONDS");
        if (dataReceivingTimeInSeconds == null || dataReceivingTimeInSeconds.equals("")) {
            throw new IllegalStateException("DATA_RECEIVING_TIME_IN_SECONDS environment variable must be set");
        }
        int dataReceivingSleep = Integer.parseInt(dataReceivingTimeInSeconds);

        SparkConf conf = new SparkConf().setAppName("BigData-3").setMaster(sparkMasterUrl);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(dataReceivingSleep * 1000));

        Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
        Collection<String> topics = Collections.singletonList(Topic);

        JavaInputDStream<ConsumerRecord<Object, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);
        JavaDStream<EventData> eventData = receivedData.map(EventData::CreateEventData);
        JavaDStream<EventData> congestion = eventData.filter(ed -> ed != null && ed.getType().equals("Congestion"));
        JavaPairDStream<String, String> congestionWithDuration = congestion.mapToPair(c -> new Tuple2<>(c.getEventId(), c.getCity()));

        congestionWithDuration.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Map<String, Object> getKafkaParams(String brokers) {
        Map<String, Object> kafkaParams = new HashMap<>();
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
