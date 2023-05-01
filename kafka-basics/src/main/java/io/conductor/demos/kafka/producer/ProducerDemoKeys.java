package io.conductor.demos.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

@Slf4j
public class ProducerDemoKeys {
    public static void main(String[] args) {
        log.info("in Kafka Producer");
        //create producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties); //use try-with-resources

        IntStream.rangeClosed(0, 10)
                .forEach(i -> {
                    //create a producer record
                    String topic = "demo_java";
                    String value = "hello world" + i;
                    String key =  "id_" + i;

                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(topic, key, value);

                    //send the data
                    producer.send(producerRecord, (metadata, exception) -> {
                        //executes every time a message sent
                        Optional.ofNullable(exception)
                                .ifPresentOrElse(exception1 -> log.error("Error while producing", exception1),
                                        () -> log.info("Received new metadata.\n" +
                                                        "Topic: {}\n" +
                                                        "Key: {}\n" +
                                                        "Partition: {}\n" +
                                                        "Offset: {}\n" +
                                                        "Timestamp: {}",
                                                metadata.topic(),
                                                producerRecord.key(),
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
