package org.flink.sink;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;

public class IgnoringExceptionFailureHandler implements ActionRequestFailureHandler {
    @Override
    public void onFailure(
            ActionRequest action,
            Throwable failure,
            int restStatusCode,
            RequestIndexer indexer) throws Throwable {

        if (ExceptionUtils.findThrowable(failure, ElasticsearchException.class).isPresent()) {
            return;
        }
        throw failure;
    }
}