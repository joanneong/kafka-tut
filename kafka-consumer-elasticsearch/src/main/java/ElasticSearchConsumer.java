import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    final static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "twitter_tweets";
    private static final String GROUP = "kafka-demo-elasticsearch";

    public static ElasticsearchClient getClient() {
        // create low level client
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();

        // create transport layer with Jackson mapper
        ElasticsearchTransport elasticsearchTransport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // create API client
        ElasticsearchClient client = new ElasticsearchClient(elasticsearchTransport);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        // create consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // do manual committing to ensure no data loss
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) {
        ElasticsearchClient client = getClient();

        KafkaConsumer<String, String> consumer = createConsumer(TOPIC);
        ObjectMapper objectMapper = new ObjectMapper();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + " records");
            for (ConsumerRecord<String, String> record : records) {
                Tweet tweet = null;
                try {
                    tweet = objectMapper.readValue(record.value(), Tweet.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                // insert data into Elasticsearch
                Tweet finalTweet = tweet;
                IndexRequest<Tweet> indexRequest = IndexRequest.of(builder -> builder
                        .index("twitter")
                        .document(finalTweet)
                        .id(finalTweet.getId())); // ensure idempotency - insert only once for duplicate tweets

                IndexResponse indexResponse = null;
                try {
                    indexResponse = client.index(indexRequest);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                String id = indexResponse == null ? "" : indexResponse.id();
                logger.info(id);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
            logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }

//        client.shutdown();
    }
}
