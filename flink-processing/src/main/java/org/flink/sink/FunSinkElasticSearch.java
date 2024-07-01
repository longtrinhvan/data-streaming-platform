package org.flink.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.flink.model.Message;


public class FunSinkElasticSearch implements ElasticsearchSinkFunction<Message> {

    private static final long serialVersionUID = 1L;

    public UpdateRequest updateRequest(Message message) {
        return new UpdateRequest(message.getIndexName(), message.getIdentify().trim())
                .doc(message.getData(), XContentType.JSON)
                .docAsUpsert(true);
    }

    public DeleteRequest deleteRequest(Message message) {
        return new DeleteRequest(message.getIndexName(), message.getIdentify().trim());
    }

    @Override
    public void process(Message message, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        if ("delete".equals(message.getAction())) {
            requestIndexer.add(deleteRequest(message));
        } else {
            requestIndexer.add(updateRequest(message));
        }
    }
}
