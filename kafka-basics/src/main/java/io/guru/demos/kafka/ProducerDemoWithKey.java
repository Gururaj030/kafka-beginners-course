package io.guru.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());

    public static void main(String[] args) {

        // Create Producer Properties
        Properties properties = new Properties();

        // Set the Producer properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create the Producer Record
        ProducerRecord<String, String> producerRecord;
        String topic = "demo_java1";

        for (int j = 0; j <= 2; j++) {
            for (int i = 1; i <= 10; i++) {
                String key = "id_" + i;
                String message = "hello world " + i;
                producerRecord = new ProducerRecord<>(topic, key, message);
                // Send the data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            logger.info("Recieved metadata \n" +
                                    "Key: " + key + " |" +
                                    "Partition: " + recordMetadata.partition() + "\n"
                            );
                        } else {
                            logger.error("Error while producing a message", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Tell the producer to send all the data until done - Synchronous
        producer.flush();

        // Close the producer
        producer.close();
    }
}
