package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallbackDemo.class);

    public static void main(String[] args) {
        log.info("I am a producer!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "40");

        //Dont user in prod
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world");

        for (int j = 0; j < 20; j++) {
            for (int i = 0; i < 20; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world " + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // Executes every time a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            log.info("Received new metadata. \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });

            }
        }




//        flush and close the producer
        producer.flush();
        producer.close();
    }
}
