package com.usp;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class OrderEngineApplication {

    @Value("${order-submission.topic-name}")
    private String topicName;

    @Value("${order-submission.num-partitions}")
    private String numPartitions;

    @Value("${order-submission.num-replicas}")
    private String numReplicas;

    public static void main(String[] args) {
        SpringApplication.run(OrderEngineApplication.class, args);
    }


    @Bean
    public NewTopic topic() {
        return new NewTopic(topicName, Integer.valueOf(numPartitions), Short.valueOf(numReplicas));
    }


}
