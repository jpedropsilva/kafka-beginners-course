package org.example.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;


import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    static void main() throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        //create produce proeprties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //set high throughput producer configs
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Headers headers = Headers.of("User-Agent", "Kafka-Wikimedia-Client/1.0 (https://example.com)");

        URI uri = URI.create(url);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, uri);
        EventSource eventSource = builder.headers(headers).build();


        eventSource.start();

        // we produce for 10 minutes
        TimeUnit.MINUTES.sleep(10);
    }
}
