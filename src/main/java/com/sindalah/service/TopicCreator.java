package com.sindalah.service;

public interface TopicCreator {
    String createTopic(String topicName, int numPartitions) throws Exception ;

}
