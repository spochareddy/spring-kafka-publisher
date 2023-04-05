package com.sindalah.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.sindalah.service.KafkaConsumer;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaConsumerImpl implements KafkaConsumer {

    @Value( "${spring.kafka.consumer.group-id[0]}")
    String group1;

    @Value( "${spring.kafka.consumer.group-id[1]}")
    String group2;

    //@KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id[0]}" )
    public void consumeMessages(String msg) {
        log.info("Consumed messages from " + group1 + " = " + msg);
    }

    @KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id[1]}")
    public void consumeMessages1(String msg) {
        log.info("Consumed messages from " + group2 + " = " + msg);
    }

}
