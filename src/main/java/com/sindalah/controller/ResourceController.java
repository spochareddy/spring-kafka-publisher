package com.sindalah.controller;

import com.sindalah.service.KafkaSubscriber;
import com.sindalah.service.TopicCreator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.sindalah.service.KafkaProducer;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class ResourceController {
    @Autowired
    KafkaProducer kafkaproducer;
    @Autowired
    KafkaSubscriber kafkaSubscriber;
    @Autowired
    TopicCreator topicCreator;

    @GetMapping("/producer")
    public String produceMessages(String message) {
        kafkaproducer.sendMessages(message);
        return "Messages sent to the Kafka Topic Successfully";
    }

    @GetMapping("/subscriber")
    public List<String> subscribeMessage(@RequestParam("topic") String topicName, @RequestParam("groupId") String groupId) {
        return kafkaSubscriber.fetchRecords(topicName,groupId);
    }

    @GetMapping("/topic")
    public String createTopic(@RequestParam("topic") String topicName, @RequestParam("partitions") int partitions) throws Exception {
        return topicCreator.createTopic(topicName, partitions);
    }
}


