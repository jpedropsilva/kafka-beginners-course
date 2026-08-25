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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
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

public class OpenSearchFalseAutoCommitBulkRequestGracefullShutdownConsumer {

    private static final Logger logger = LoggerFactory.getLogger(
            OpenSearchFalseAutoCommitBulkRequestGracefullShutdownConsumer.class);

    static void main() throws IOException {

        //create opensearch openSearchClient
        RestHighLevelClient openSearchClient = createClient();


        //create kafka openSearchClient
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();


        final Thread mainThread = Thread.currentThread();
        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detecting shutdown, exit by callingg consumer.wakeup() ");
            kafkaConsumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try (openSearchClient; kafkaConsumer) {
            boolean wikimediaExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!wikimediaExist) {
                //create a index on OpenSearch
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
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

                BulkRequest bulkRequest = new BulkRequest();


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
                        bulkRequest.add(indexRequest);
//                        logger.info("Index Response id: " + id);
                    } catch (Exception e) {
                        logger.error("Error indexing record: " + e.getMessage());
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Bulk Response: " + bulkResponse.getItems().length + " items indexed");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                //At leat once
                //Only commit when all records are processed
                //commit the offsets
                kafkaConsumer.commitSync();
                logger.info("Offsets committed");
            }
        }catch (WakeupException e) {
            logger.info("WakeupException detected, exiting...");
        } catch (Exception e) {
            logger.error("Unexpected exception occurred: " + e.getMessage());
        } finally {
            kafkaConsumer.close(); //close the consumer and commit the offsets
            openSearchClient.close();
            logger.info("Consumer gracefully closed");
        }

        //close resources
//        openSearchClient.close();
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
