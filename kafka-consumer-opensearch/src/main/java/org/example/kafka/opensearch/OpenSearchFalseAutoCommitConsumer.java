package org.example.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;

public class OpenSearchFalseAutoCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchFalseAutoCommitConsumer.class);

    static void main() throws IOException {

        //create opensearch client
        RestHighLevelClient client = createClient();


        //create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();


        try (client; kafkaConsumer) {
            boolean wikimediaExist = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!wikimediaExist) {
                //create a index on OpenSearch
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Index created");
            } else {
                logger.info("Index already exists");
            }

            //subscribe to a topic
            kafkaConsumer.subscribe(java.util.Collections.singletonList("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                records.count();
                logger.info("Number of records: " + records.count());

                for (ConsumerRecord<String, String> record : records) {

                    //make idenpotent
                    //Strategy 1
                    //String id = record.topic() + "-" + record.partition() + "-" + record.offset();

                    //Strategy 2
                    //Inspect data and check if data provides ID
                    //Ir ao konduktor e ver uma mensagem
                    String id = extractId(record.value());

                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia");
                        indexRequest.id(id);
                        indexRequest.source(record.value(), XContentType.JSON);
                        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info("Index Response id: " + indexResponse.getId());
                    } catch (Exception e) {
                        logger.error("Error indexing record: " + e.getMessage());
                    }
                }
                //At leat once
                //Only commit when all records are processed
                //commit the offsets
                kafkaConsumer.commitSync();
                logger.info("Offsets committed");
            }
        }

        //close resources
//        client.close();
//        kafkaConsumer.close();

    }

    private static String extractId(String value) {
        return JsonParser.parseString(value).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        String groupId = "my-fourth-application";
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", "false");

        //none,earliest,latest
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }

    public static RestHighLevelClient createClient() {
        String connString = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;

        URI conUri = URI.create(connString);

        String userInfo = conUri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(conUri.getHost(), conUri.getPort(), conUri.getScheme())));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(conUri.getHost(), conUri.getPort(), conUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(
                                                    credentialsProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }
}
