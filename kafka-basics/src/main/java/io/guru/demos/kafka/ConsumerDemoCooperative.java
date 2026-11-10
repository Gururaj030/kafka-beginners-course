package io.guru.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // Set consumer group id
        properties.setProperty("group.id", groupId);

        // set the offset
        properties.setProperty("auto.offset.reset", "earliest");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to a main thread
        final Thread mainThread = Thread.currentThread();

        // Adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected shutdown. Let's exit b calling consumer.wakeup() ");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            logger.info("polling");
            // subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + " Value: " + record.value());
                    logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumer is trying to shutdown");
        } catch (Exception e) {
            logger.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offset
            logger.info("Consumer is gracefully shutdown");
        }
    }
}
