package com.sindalah.service.impl;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.sindalah.service.KafkaProducer;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaProducerImpl implements KafkaProducer {
    private final static int PARTITION_COUNT = 1;
    private final static int MSG_COUNT = 5;
    @Value("${topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void sendMessages(String message) {
        int key = 0;
        for (int i = 0; i < MSG_COUNT; i++) {
            for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                String value = message + "-" + i;
                key++;
                log.info("Sending messages to topic:: " + topicName + " ,key ::" + key + " ,value ::" + value + " ,partition id ::" + partitionId);
                kafkaTemplate.send(new ProducerRecord<>(topicName, partitionId,
                        Integer.toString(key), value));
            }
        }
    }
}
