package org.flink.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flink.model.Message;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DeserializationKafka implements DeserializationSchema<Message> {
    private static final long serialVersionUID = 3424321L;
    private final ObjectMapper mapper;

    public DeserializationKafka() {
        this.mapper = Common.generateJsonMapper();
    }

    @Override
    public Message deserialize(final byte[] bytes) {
        byte[] jsonBytes = Objects.requireNonNull(bytes);
        String jsonStr = new String(jsonBytes, StandardCharsets.UTF_8);
        try {
            return mapper.readValue(jsonStr, Message.class);
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(final Message nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(Message.class);
    }
}
