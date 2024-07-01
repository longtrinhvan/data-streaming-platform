package org.akka;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import com.typesafe.config.Config;
import org.akka.process.KafkaMessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;

@SpringBootApplication
public class MainApplication implements CommandLineRunner {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int PARALLELISM = 1;

    @Autowired
    private KafkaMessageProcessor kafkaMessageProcessor;

    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        ActorSystem system = actorSystem();
        ConsumerSettings<String, String> consumerSettings = consumerSettings(system);
        CommitterSettings committerSettings = committerSettings(system);

        Consumer.DrainingControl<Done> control = Consumer
                .committableSource(
                        consumerSettings.withStopTimeout(Duration.ZERO),
                        Subscriptions.topics("akka"))
                .mapAsync(PARALLELISM, msg -> kafkaMessageProcessor.processMessage(msg.record().key(), msg.record().value())
                        .<ConsumerMessage.Committable>thenApply(done -> msg.committableOffset()))
                .toMat(Committer.sink(committerSettings), Consumer::createDrainingControl)
                .run(system);

        String controlName = control.toString();
        LOGGER.error(controlName);
    }

    @Bean
    public ActorSystem actorSystem() {
        return ActorSystem.create("akka");
    }

    @Bean
    public ConsumerSettings<String, String> consumerSettings(ActorSystem system) {
        return ConsumerSettings
                .create(system, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers("localhost:9092")
                .withGroupId("streaming")
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
    }

    @Bean
    public CommitterSettings committerSettings(ActorSystem system) {
        Config config = system.settings().config().getConfig("akka.kafka.committer");
        return CommitterSettings.create(config);
    }
}
