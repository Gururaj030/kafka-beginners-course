package io.guru.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("ConsumerDemo: Hello I am starting Kakfa Consumer");

        String topic = "demo_java";

        // Create Consumer Properties
        Properties properties = new Properties();

        // Set the Consumer properties
        String groupId = "my-java-application";
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Set consumer group id
        properties.setProperty("group.id", groupId);

        // set the offset
        properties.setProperty("auto.offset.reset", "earliest");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            logger.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records ) {
                logger.info("Key: " + record.key()  + " Value: " + record.value());
                logger.info("Partition: " + record.partition()  + " Offset: " + record.offset());
            }
        }
    }
}
