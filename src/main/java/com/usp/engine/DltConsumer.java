package com.usp.engine;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
public class DltConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DltConsumer.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    private KafkaConsumer<String, String> kafkaConsumer;

    @Value("${order-submission.topic-name}")
    private String topicName;
    @Value("${order-submission.topic-name-dlt}")
    private String topicDltName;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;


    public DltConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "order-submission-consumer-DLT");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Arrays.asList("orders.DLT"));
    }


    public void runSingleWorker() {


        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            logger.info("Received message: " + message);

            message = "success";
            kafkaTemplate.send("orders", message);

            Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

            commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));

            kafkaConsumer.commitSync(commitMessage);

            logger.info("Offset committed to Kafka.");

        }
    }

}
