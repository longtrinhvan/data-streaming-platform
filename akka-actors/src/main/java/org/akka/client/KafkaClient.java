package org.akka.client;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class KafkaClient {

    ActorSystem actorApplication;

    @Autowired
    KafkaClient(ActorSystem actorApplication) {
        this.actorApplication = actorApplication;
    }

    private ConsumerSettings<String, String> productKafkaConsumerSettings;

    private Config kafkaConfig;

    @Value("${application.kafka.groupId}")
    public String groupId;

    @Value("${application.kafka.topic}")
    public String productTopic;

    @Autowired
    @PostConstruct
    public void initializeKafkaConfigs() {
        this.productTopic = productTopic;
        this.kafkaConfig = this.actorApplication.settings().config().getConfig("akka.kafka.committer");
        this.productKafkaConsumerSettings = ConsumerSettings
                .create(actorApplication, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers("localhost:9092")
                .withGroupId(groupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public String getProductTopic() {
        return this.productTopic;
    }

    @Bean(name = "kafkaConsumerSettings")
    public ConsumerSettings<String, String> kafkaConsumerSettings() {
        return this.productKafkaConsumerSettings;
    }

    @Bean(name = "kafkaConsumerConfigs")
    public Config kafkaConfigs() {
        return this.kafkaConfig;
    }
}