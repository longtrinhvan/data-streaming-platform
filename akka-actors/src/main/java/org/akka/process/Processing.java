package org.akka.process;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerMessage;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.alpakka.elasticsearch.*;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow;
import akka.stream.alpakka.mongodb.javadsl.MongoSink;
import akka.stream.javadsl.Source;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.akka.client.KafkaClient;
import org.akka.common.ObjectMapperProvider;
import org.akka.model.Message;
import org.akka.service.BusinessService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class Processing {

    @Qualifier("actorApplication")
    private ActorSystem actorApplication;
    @Autowired
    private BusinessService businessService;

    @Autowired
    private ObjectMapperProvider objectMapperProvider;

    @Autowired
    private KafkaClient kafkaClient;
    private static final Logger LOGGER = LogManager.getLogger();
    @Qualifier("productCollection")
    private MongoCollection<BsonDocument> productCollection;

    private ElasticsearchConnectionSettings connectionSettings;

    @Value("${application.elasticsearch.host}")
    public String elasticsearchHost;

    @Value("${application.elasticsearch.index}")
    public String elasticsearchIndex;

    @Autowired
    private void processingOk(
            ActorSystem actorApplication,
            KafkaClient kafkaClient,
            MongoCollection<BsonDocument> productCollection,
            ObjectMapperProvider objectMapperProvider) {
        this.actorApplication = actorApplication;
        this.objectMapperProvider = objectMapperProvider;
        this.kafkaClient = kafkaClient;
        this.productCollection = productCollection;
        this.connectionSettings = ElasticsearchConnectionSettings.create(elasticsearchHost);
    }

    public void streams() {

//        Consumer.DrainingControl<Done> control = Consumer
//                .committableSource(
//                        kafkaClient.kafkaConsumerSettings().withStopTimeout(Duration.ZERO),
//                        Subscriptions.topics(kafkaClient.getProductTopic())
//                )
//                .map(committable -> {
//                    committable.committableOffset();
//                    return committable;
//                }).mapAsync(1, msg -> businessService.major(msg.record())
//                        .thenApply(done -> msg.committableOffset()))
//                .toMat(Committer.sink(CommitterSettings.create(kafkaClient.kafkaConfigs())), Consumer::createDrainingControl)
//                .run(actorApplication);


        Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> source = Consumer
                .committableSource(
                        kafkaClient.kafkaConsumerSettings().withStopTimeout(Duration.ZERO),
                        Subscriptions.topics(kafkaClient.getProductTopic())
                );

        // Process and commit immediately after reading from Kafka
        Source<Message, Consumer.Control> processedSource =
                source.mapAsync(1, msg -> msg.committableOffset().commitJavadsl()
                        .thenApply(done -> businessService.major(msg.record())));

        processedSource
                .map(message -> BsonDocument.parse(message.getData()))
                .mapMaterializedValue(control -> NotUsed.getInstance())
                .runWith(MongoSink.insertOne(productCollection), actorApplication);

        // Sink to MongoDB
//        processedSource.runWith(MongoSink.insertOne(productCollection), actorApplication);

        // Sink to Elasticsearch
        processedSource.map(message -> WriteMessage
                        .createUpsertMessage(message.getIdentify(),
                                objectMapperProvider.readValue(
                                        message.getData(),
                                        Message.class)))
                .via(ElasticsearchFlow.create(
                        ElasticsearchParams.V5("product", "_doc"),
                        ElasticsearchWriteSettings.create(connectionSettings)
                                .withBufferSize(5)
                                .withApiVersion(ApiVersion.V5),
                        objectMapperProvider.getObjectMapper()))
                .map(writeResult -> {
                    writeResult
                            .getError()
                            .ifPresent(errorJson -> LOGGER.error(writeResult.getErrorReason().orElse(errorJson)));
                    return NotUsed.notUsed();
                })
                .run(actorApplication);

    }
}