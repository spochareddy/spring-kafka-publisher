package com.sindalah.service.impl;

import com.sindalah.service.TopicCreator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TopicCreatorImpl implements TopicCreator {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String brokers;

    @Override
    public String createTopic(String topicName, int numPartitions) throws Exception {

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        AdminClient admin = AdminClient.create(config);

        //checking if topic already exists
        boolean alreadyExists = admin.listTopics().names().get().stream()
                .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
        if (alreadyExists) {
            log.info("topic already exits::" + topicName);
        } else {
            //creating new topic
            log.info("creating topic::", topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }

        //describing topic
        log.info("-- describing topic --");
        admin.describeTopics(Collections.singleton(topicName)).all().get()
                .forEach((topic, desc) -> {
                    log.info("Topic:: " + topic);
                    log.info("Partitions:: " + desc.partitions().size() + " partition ids:: " +
                            desc.partitions()
                                    .stream()
                                    .map(p -> Integer.toString(p.partition()))
                                    .collect(Collectors.joining(",")));
                });
        admin.close();
        return topicName;
    }
}
