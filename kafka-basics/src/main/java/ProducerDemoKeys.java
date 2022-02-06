import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * key 9 - partition 2
 * key 8 - partition 2
 * key 7 - partition 2
 * key 6 - partition 0
 * key 5 - partition 2
 * key 4 - partition 0
 * key 3 - partition 0
 * key 2 - partition 2
 * key 1 - partition 2
 * key 0 - partition 2
 */

public class ProducerDemoKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // send data - asynchronous, you have to flush data
        for (int i = 0; i < 10; i++) {
            String topic = "some_topic_2";
            String value = "hello from the Java side - " + i;
            String key = "key id_" + i;

            logger.info("Key: " + key + "\n");

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata.\n"
                            + "Topic: " + recordMetadata.topic() + "\n"
                            + "Partition: "  + recordMetadata.partition() + "\n"
                            + "Offset: " + recordMetadata.offset() + "\n"
                            + "Timestamp: " + recordMetadata.timestamp() + "\n"
                    );
                } else {
                    logger.error("Error while producing to Kafka", e);
                }
            });

            producer.flush();
        }
    }
}
