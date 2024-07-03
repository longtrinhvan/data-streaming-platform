package org.akka.service;

import org.akka.common.ObjectMapperProvider;
import org.akka.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


@Service
public class BusinessService {

    private final ObjectMapperProvider objectMapperProvider;
    private static final Logger LOGGER = LogManager.getLogger();

    @Autowired
    public BusinessService(ObjectMapperProvider objectMapperProvider) {
        this.objectMapperProvider = objectMapperProvider;
    }

    public Message major(ConsumerRecord<String, String> consumerRecord) {
        LOGGER.info(consumerRecord.value());
        return objectMapperProvider.readValue(consumerRecord.value(), Message.class);
    }
}
