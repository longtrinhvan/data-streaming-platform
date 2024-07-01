package org.flink.common;

import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.flink.model.Message;
import org.flink.sink.FunSinkElasticSearch;
import org.flink.sink.IgnoringExceptionFailureHandler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
public class Elasticsearch {

    public static List<HttpHost> httpHosts(String addressList) {
        return Arrays.stream(addressList.split(","))
                .map(item -> item.split(":"))
                .map(subItem -> new HttpHost(subItem[0].trim(),
                        Integer.parseInt(subItem[1].trim()),
                        "http"))
                .collect(Collectors.toList());
    }

    public static ElasticsearchSink<Message> sinkFunction(String elasticsearchHost) {
        List<HttpHost> httpHosts = httpHosts(elasticsearchHost);
        var esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new FunSinkElasticSearch());
        esSinkBuilder.setBulkFlushInterval(500);
        esSinkBuilder.setBulkFlushBackoff(true);
        esSinkBuilder.setBulkFlushBackoffRetries(3);
        esSinkBuilder.setBulkFlushBackoffDelay(1000);
        esSinkBuilder.setFailureHandler(new IgnoringExceptionFailureHandler());
        return esSinkBuilder.build();
    }
}
