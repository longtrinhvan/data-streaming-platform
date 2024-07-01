package org.flink.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flink.common.Common;
import org.flink.model.Message;

public class ProcessData extends ProcessWindowFunction<Message, Message, String, TimeWindow> {

    private static final Logger LOGGER = LogManager.getLogger(ProcessData.class);

    private static final long serialVersionUID = 11231231002321L;

    @Override
    public void open(Configuration parameters) {
        LOGGER.info("configuration {}", parameters);
    }

    @Override
    public void process(String keyIN,
                        Context context,
                        Iterable<Message> iterable, Collector<Message> out) {
        try {
            for (Message item : iterable) {
                String data = Common.generateJsonMapper().writeValueAsString(item);
                LOGGER.info("data {}", data);
                out.collect(item);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
