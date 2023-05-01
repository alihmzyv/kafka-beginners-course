package io.conductor.demos.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemoWithShutDown {
    public static void main(String[] args) {
        log.info("in Kafka Consumer");

        Properties properties = new Properties();

        String bootstrapServers = "localhost:9092";
        String groupId = "consumer-group-2";
        String topic = "demo_java";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
            consumer.wakeup();
        }));

        //subscribe to topic
        consumer.subscribe(Collections.singletonList(topic));

        //poll for new data
        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(consumerRecord -> {
                log.info("Key: {}, Value: {}", consumerRecord.key(), consumerRecord.value());
                log.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
            });
        }
    }
}
