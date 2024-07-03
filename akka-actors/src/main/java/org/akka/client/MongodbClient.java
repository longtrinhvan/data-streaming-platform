package org.akka.client;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;


@Configuration
public class MongodbClient {

    @Value("${application.mongodb.database.name}")
    private String mongodbName;

    @Value("${application.mongodb.collection}")
    private String mongodbCollectionName;

    @Value("${application.mongodb.url}")
    private String mongodbUrl;

    private CodecRegistry codecRegistry;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;


    @PostConstruct
    public void initializeMongodbDBConfigs() {

        PojoCodecProvider codecProvider = PojoCodecProvider.builder()
                .register(BsonDocument.class)
                .build();
        this.codecRegistry = CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());

        this.mongoClient = MongoClients.create(this.mongodbUrl);
        this.mongoDatabase = this.mongoClient.getDatabase(this.mongodbName);
    }

    public CodecRegistry codecRegistry() {
        return this.codecRegistry;
    }

    public MongoClient mongodbClient() {
        return this.mongoClient;
    }

    public MongoDatabase mongodbDatabase() {
        return this.mongoDatabase;
    }

    @Bean(name = "productCollection")
    public MongoCollection<BsonDocument> messageCollection() {
        return this.mongoDatabase.getCollection("product", BsonDocument.class)
                .withCodecRegistry(this.codecRegistry);
    }
}