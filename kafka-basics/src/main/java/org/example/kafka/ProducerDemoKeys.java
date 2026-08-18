package org.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        log.info("I am a producer!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        List<String> list = Arrays.asList("demo_topic", "demo_java");

        for (String topic : list) {
            for (int i = 0; i < 20; i++) {

//                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello wolrd " +i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, "hello world " + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // Executes every time a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            log.info("key " + key + "| Partition" + metadata.partition());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });

            }
        }


        //flush and close the producer
        producer.flush();
        producer.close();
    }
}
