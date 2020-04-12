import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {
    public static KafkaProducer producer;

    public static void main(String[] args) throws Exception {

        String topic = "/user/user01/stream:ubers";
        String fileName = "/user/user01/data/uber.csv";

        if (args.length == 2) {
            topic = args[0];
            fileName = args[1];

        }
        System.out.println("Sending to topic " + topic);
        configureProducer();
        File f = new File(fileName);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {

            ProducerRecord<String, String> rec = new ProducerRecord(topic,  line);

            producer.send(rec);
            System.out.println("Sent message: " + line);
            line = reader.readLine();
            Thread.sleep(600l);

        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);
    }

    public static void configureProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "x.xx.xxx.xxx:9092");
        props.put("metadata.broker.list", "x.xx.xxx.xxx:9091, x.xx.xxx.xxx:9092, x.xx.xxx.xxx:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(props);
    }
}
