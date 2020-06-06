import java.io.*;
import java.net.URI;
import java.util.Properties;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {
    private final static String TrafficTopic = "traffic-data";
    private final static String WeatherTopic = "weather-data";

    private final static String PrimaryTrafficEvent = "T";
    private final static String PrimaryWeatherEvent = "W";
    private final static String AllEvents = "A";

    public static void main(String[] args) throws Exception {

        System.out.println("RUNNING STREAMING-PRODUCER");
        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
        if (initialSleepTime != null && !initialSleepTime.equals("")) {
            int sleep = Integer.parseInt(initialSleepTime);
            System.out.println("Sleeping on start " + sleep + "sec");
            Thread.sleep(sleep * 1000);
        }

        String primaryEventType = System.getenv("PRIMARY_EVENT_TYPE");
        if (primaryEventType == null || primaryEventType.equals("") || (!primaryEventType.equals(PrimaryTrafficEvent) && !primaryEventType.equals(PrimaryWeatherEvent) && !primaryEventType.equals(AllEvents))) {
            primaryEventType = "T";
            System.out.println("Sending Traffic event by default");
        }
        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set.");
        }
        String csvFilePath = System.getenv("CSV_FILE_PATH");
        if (csvFilePath == null || csvFilePath.equals("")) {
            throw new IllegalStateException("CSV_FILE_PATH environment variable must be set");
        }
        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }
        String dataSendingTimeInSeconds = System.getenv("DATA_SENDING_TIME_IN_SECONDS");
        if (dataSendingTimeInSeconds == null || dataSendingTimeInSeconds.equals("")) {
            throw new IllegalStateException("DATA_SENDING_TIME_IN_SECONDS environment variable must be set");
        }
        int dataSendingSleep = Integer.parseInt(dataSendingTimeInSeconds);

        KafkaProducer<String, String> producer = configureProducer(kafkaUrl);

        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FSDataInputStream inputStream = null;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfsUrl), conf);
            Path inFile = new Path(csvFilePath);
            inputStream = fs.open(inFile);
            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
                throw new IOException("Input file not found");
            }

            String line = inputStream.readLine();
            while(line != null) {
                EventData tmp = EventData.CreateEventData(line);

                if (tmp != null && !primaryEventType.equals(PrimaryWeatherEvent) && tmp.getSource().equals(EventData.SOURCE_T)) {
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(TrafficTopic, line);
                    producer.send(rec);
                    System.out.println("[KAFKA TRAFFIC DATA SENT}]: " + tmp.getEventId());
                    Thread.sleep(dataSendingSleep * 1000);
                    System.out.println("Sleeping " + dataSendingSleep + "sec");
                }

                if (tmp != null && !primaryEventType.equals(PrimaryTrafficEvent) && tmp.getSource().equals(EventData.SOURCE_W)) {
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(WeatherTopic, line);
                    producer.send(rec);
                    System.out.println("[KAFKA WEATHER DATA SENT}]: " + tmp.getEventId());
                    Thread.sleep(dataSendingSleep * 1000);
                    System.out.println("Sleeping " + dataSendingSleep + "sec");
                }
                line = inputStream.readLine();
            }

        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);
    }

    public static KafkaProducer<String, String> configureProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "streaming-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }
}
