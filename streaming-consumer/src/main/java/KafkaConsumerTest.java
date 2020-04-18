//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Properties;
//
////<dependency>
////<groupId>org.apache.kafka</groupId>
////<artifactId>kafka_2.12</artifactId>
////<version>${kafka.version}</version>
////</dependency>
////<dependency>
////<groupId>org.apache.kafka</groupId>
////<artifactId>kafka-clients</artifactId>
////<version>${kafka.version}</version>
////</dependency>
////<kafka.version>2.4.0</kafka.version>
//
//class KafkaConsumerTest {
//    private static String Topic = "test";
//
//    public static void main2(String[] args) throws Exception {
//
//        System.out.println("RUNNING STREAMING-CONSUMER");
//        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME");
//        if (initialSleepTime != null && !initialSleepTime.equals("")) {
//            int sleep = Integer.parseInt(initialSleepTime);
//            System.out.println("Sleeping on start " + sleep + "sec");
//            Thread.sleep(sleep * 1000);
//        }
//
////        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
////        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
////            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
////        }
//
//        String kafkaUrl = System.getenv("KAFKA_URL");
//        if (kafkaUrl == null || kafkaUrl.equals("")) {
//            throw new IllegalStateException("KAFKA_URL environment variable must be set");
//        }
//
//        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = configureConsumer(kafkaUrl);
//        List<String> topics = new ArrayList<String>();
//        topics.add(Topic);
//        consumer.subscribe(topics);
//
//        long pollTimeOut = 1000;
//        long waitTime = 30 * 1000;  // loop for while loop 30 seconds
//        long numberOfMsgsReceived = 0;
//        while (true) {
//            // Request unread messages from the topic.
//            ConsumerRecords<String, String> msg = consumer.poll(pollTimeOut);
//            if (msg.count() == 0) {
//            } else {
//                numberOfMsgsReceived += ((ConsumerRecords) msg).count();
//
//                Iterator<ConsumerRecord<String, String>> iter = msg.iterator();
//                while (iter.hasNext()) {
//                    ConsumerRecord<String, String> record = iter.next();
//                    System.out.println(record.value());
//
//                }
//            }
//            waitTime = waitTime - 1000; // decrease time for loop
//        }
//    }
//
//    public static org.apache.kafka.clients.consumer.KafkaConsumer<String, String> configureConsumer(String brokers) {
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//
//        return new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
//    }
//}
