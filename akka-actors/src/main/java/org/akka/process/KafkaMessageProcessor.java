package org.akka.process;

import akka.Done;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Service
public class KafkaMessageProcessor {

    private static final Logger LOGGER = LogManager.getLogger();

    public CompletionStage<Done> processMessage(String key, String value) {
        LOGGER.error("key: {}  value: {}", key, value);
        return CompletableFuture.completedFuture(Done.getInstance());
    }

    public ConsumerRecord<String, String> processMessageV2(ConsumerRecord<String, String> consumerRecord) {
        LOGGER.error("key: {}  value: {}", consumerRecord.key(), consumerRecord.value());
        return consumerRecord;
    }
}

