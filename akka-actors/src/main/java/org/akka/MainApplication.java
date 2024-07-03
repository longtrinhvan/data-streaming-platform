package org.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.alpakka.elasticsearch.*;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow;
import akka.stream.javadsl.Keep;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.akka.model.Message;
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

import static org.akka.config.ConfigEnv.*;

@SpringBootApplication
public class MainApplication implements CommandLineRunner {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int PARALLELISM = 1;

    @Autowired
    private KafkaMessageProcessor kafkaMessageProcessor;
    @Autowired
    ObjectMapper generateJsonMapper;

    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        ActorSystem system = actorSystem();
        ConsumerSettings<String, String> consumerSettings = consumerSettings(system);
        CommitterSettings committerSettings = committerSettings(system);


//        Consumer.DrainingControl<Done> control = Consumer
//                .committableSource(
//                        consumerSettings.withStopTimeout(Duration.ZERO),
//                        Subscriptions.topics("flink"))
//                .mapAsync(PARALLELISM, msg -> kafkaMessageProcessor.processMessage(msg.record().key(), msg.record().value())
//                        .<ConsumerMessage.Committable>thenApply(done -> msg.committableOffset()))
//                .toMat(Committer.sink(committerSettings), Consumer::createDrainingControl)
//                .run(system);

        ElasticsearchConnectionSettings connectionSettings = ElasticsearchConnectionSettings
                .create(ELASTIC_SEARCH_HOST)
                ;

        Consumer.DrainingControl<Done> control = Consumer
                .committableSource(
                        consumerSettings.withStopTimeout(Duration.ZERO),
                        Subscriptions.topics(TOPIC)
                )
                .asSourceWithContext(ConsumerMessage.CommittableMessage::committableOffset)
                .map(consumerRecord -> {
                    Message message = generateJsonMapper.readValue(consumerRecord.record().value(), Message.class);
                    System.out.println(generateJsonMapper.writeValueAsString(message));
                    return WriteMessage.createUpsertMessage(message.getIdentify(),
                            generateJsonMapper.readValue(
                                    message.getData(),
                                    Message.class));
                })
                .via(ElasticsearchFlow
                        .createWithContext(
                                ElasticsearchParams.V5(ELASTIC_SEARCH_INDEX,"_doc"),
                                ElasticsearchWriteSettings.create(connectionSettings)
                                        .withBufferSize(5)
                                        .withApiVersion(ApiVersion.V5),
                                generateJsonMapper))

                .map(writeResult -> {
                    writeResult
                            .getError()
                            .ifPresent(errorJson -> {
                                throw new RuntimeException("Elasticsearch update failed " + writeResult.getErrorReason().orElse(errorJson));
                            });
                    return NotUsed.notUsed();
                })
                .toMat(Committer.sinkWithOffsetContext(CommitterSettings.create(system)), Keep.both())
                .mapMaterializedValue(Consumer::createDrainingControl)
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
                .withGroupId(GROUP_ID)
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
    }

    @Bean
    public CommitterSettings committerSettings(ActorSystem system) {
        Config config = system.settings().config().getConfig("akka.kafka.committer");
        return CommitterSettings.create(config);
    }
}
