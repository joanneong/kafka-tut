import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static final String API_KEY = "API_KEY";
    public static final String API_KEY_SECRET = "API_KEY_SECRET";
    public static final String ACCESS_TOKEN = "ACCESS_TOKEN";
    public static final String ACCESS_TOKEN_SECRET = "ACCESS_TOKEN_SECRET";

    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    List<String> terms = Lists.newArrayList("kafka", "bitcoin", "Singapore");

    public TwitterProducer() {}

    public void run() {
        logger.info("Setting up...");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // create a twitter client
        Client hosebirdClient = createTwitterClient(msgQueue);
        hosebirdClient.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from twitter...");
            hosebirdClient.stop();
            logger.info("Shutting down kafka producer");
            producer.close();
            logger.info("Application has been stopped.");
        }));

        // loop to send tweets to kafka
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened: ", e);
                        }
                    }
                });
            }
        }
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // create high throughput producer (at the expense of some latency and CPU usage)
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Dotenv dotenv = Dotenv.configure()
                .directory("./kafka-twitter-producer")
                .ignoreIfMalformed()
                .ignoreIfMissing()
                .load();

        String consumerKey = dotenv.get(API_KEY);
        String consumerSecret = dotenv.get(API_KEY_SECRET);
        String token = dotenv.get(ACCESS_TOKEN);
        String secret = dotenv.get(ACCESS_TOKEN_SECRET);

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Client hosebirdClient = null;
        try {
            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

            ClientBuilder clientBuilder = new ClientBuilder()
                    .name("My kafka-twitter client")
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            hosebirdClient = clientBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hosebirdClient;
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }
}
