package org.flink.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConfigEnv {

    public static final String ELASTIC_SEARCH_HOST = "localhost:9200";
    //    private static final String BROKERS = "http://kafka:9093";
    public static final String BROKERS = "http://localhost:9092";
    public static final String TOPIC = "flink";
    public static final String GROUP_ID = "streaming";

    public static StreamExecutionEnvironment initEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5000));
        return env;
    }

}
