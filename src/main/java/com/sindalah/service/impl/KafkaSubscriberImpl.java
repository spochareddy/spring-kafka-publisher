package com.sindalah.service.impl;

import com.sindalah.service.KafkaSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
@Slf4j
public class KafkaSubscriberImpl implements KafkaSubscriber {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String brokers;
    public List<String> fetchRecords(String topicName, String groupId) {

        KafkaConsumer kafkaConsumer = new KafkaConsumer(populatePros(groupId));
        kafkaConsumer.subscribe(List.of(topicName));
        List<String> msgList = new ArrayList<>();
        // while(true) {
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
        log.info("Polling messages count ::" + consumerRecords.count());
        if(consumerRecords.isEmpty())
            consumerRecords = kafkaConsumer.poll(100);
        
        for (ConsumerRecord<String, String> r : consumerRecords) {
            log.info("key::" + r.key() + " ,value::" + r.value() + " ,PartitionId::" + r.partition() + " ,Offset::" + r.offset());
            msgList.add("offset::" + r.offset() + " ::value ::" + r.value());
        }
        kafkaConsumer.close();
        // }
        return msgList;
    }

    private Properties populatePros(final String groupId) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupId + "-" + 1);
        return properties;
    }

}
