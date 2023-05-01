package io.conductor.demos.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.replica.PartitionView;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
        log.info("in Kafka Producer");
        //create producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties); //use try-with-resources

        //create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        //send the data
        IntStream.rangeClosed(0, 10)
                .forEach(index -> {
                    producer.send(producerRecord, (metadata, exception) -> {
                        //executes every time a message sent
                        Optional.ofNullable(exception)
                                .ifPresentOrElse(exception1 -> log.error("Error while producing", exception1),
                                        () -> log.info("Received new metadata.\n" +
                                                        "Topic: {}\n" +
                                                        "Partition: {}\n" +
                                                        "Offset: {}\n" +
                                                        "Timestamp: {}",
                                                metadata.topic(),
                                                metadata.partition(),
                                                metadata.offset(),
                                                metadata.timestamp()));
                    });

                    //flush data
                    producer.flush();
                });

        //close producer
        producer.close(); //flushes
    }
}
