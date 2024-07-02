package org.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.flink.common.DeserializationKafka;
import org.flink.model.Message;
import org.flink.process.ProcessData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import static org.flink.common.Elasticsearch.sinkFunction;
import static org.flink.config.ConfigEnv.*;

@Configuration
public class DataStreamJob {

    @Bean
    public void run() {

        try {

            StreamExecutionEnvironment environment = initEnvironment();
            ElasticsearchSink<Message> elasticsearchSink = sinkFunction(ELASTIC_SEARCH_HOST);

            KafkaSource<Message> source = KafkaSource.<Message>builder()
                    .setBootstrapServers(BROKERS)
                    .setTopics(TOPIC)
                    .setGroupId(GROUP_ID)
                    .setStartingOffsets(OffsetsInitializer
                            .committedOffsets(OffsetResetStrategy.EARLIEST))
                    .setValueOnlyDeserializer(new DeserializationKafka())
                    .setProperty("partition.discovery.interval.ms", "30000")
                    .build();

            DataStream<Message> dataStream = environment.fromSource(
                    source,
                    WatermarkStrategy.noWatermarks(),
                    "Kafka Source");

            DataStream<Message> resultStream = dataStream
                    .keyBy(Message::getIdentify)
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                    .trigger(ProcessingTimeTrigger.create())
                    .process(new ProcessData())
                    .name(ProcessData.class.getSimpleName());

            resultStream.addSink(elasticsearchSink).name("Elasticsearch Sink");

            environment.execute("Flink Job");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
