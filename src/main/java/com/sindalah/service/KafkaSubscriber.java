package com.sindalah.service;

import java.util.List;

public interface KafkaSubscriber {

    public List<String> fetchRecords(final String topicName, final String groupId);
}
