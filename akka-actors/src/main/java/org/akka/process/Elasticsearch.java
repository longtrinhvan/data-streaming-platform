package org.akka.process;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class Elasticsearch {

    public static void main(String[] args) {
        try (RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"))
                .build()) {

            // Create the index request
            CreateIndexRequest request = new CreateIndexRequest("my_index");

            // Configure settings and mappings
            request.settings(Settings.builder()
                    .put("index.analysis.filter.ascii_folding.type", "asciifolding")
                    .put("index.analysis.filter.ascii_folding.preserve_original", true)
                    .put("index.analysis.analyzer.folding.tokenizer", "standard")
                    .putList("index.analysis.analyzer.folding.filter", "lowercase", "ascii_folding")
            );

            request.mapping("{\n" +
                    "  \"properties\": {\n" +
                    "    \"name\": {\n" +
                    "      \"type\": \"text\",\n" +
                    "      \"analyzer\": \"folding\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", XContentType.JSON);
            RestClients.create(config).rest();
            // Initialize the high-level client
            RestHighLevelClient client = new RestHighLevelClientBuilder(restClient).build();

            // Send the create index request and get the response
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

            // Check the acknowledgment
            boolean acknowledged = createIndexResponse.isAcknowledged();
            boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();

            System.out.println("Index creation acknowledged: " + acknowledged);
            System.out.println("Shards acknowledgment: " + shardsAcknowledged);

            // Close the client
            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}