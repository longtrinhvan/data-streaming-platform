package org.akka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

@Getter
@Builder
@Setter(AccessLevel.PUBLIC)
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    @JsonProperty("identify")
    public String identify;

    @JsonProperty("indexName")
    public String indexName;

    @JsonProperty("data")
    public String data;

    @JsonProperty("action")
    public String action;

    public static void main(String[] args) throws JsonProcessingException {

        var mapper = new ObjectMapper();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        mapper.setDateFormat(df);
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Message message = new Message();
        message.setIdentify("1");
        message.setIndexName("product");
        message.setAction("create");
        message.setData(mapper.writeValueAsString(message));
        String data = mapper.writeValueAsString(message);
        System.out.println(data);
    }

}

