package org.akka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

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

//    public static void main(String[] args) throws JsonProcessingException {
//        Message message = new Message();
//        message.setIdentify("1");
//        message.setIndexName("product");
//        message.setAction("create");
//        message.setData(Common.generateJsonMapper().writeValueAsString(message));
//        String data = Common.generateJsonMapper().writeValueAsString(message);
//        System.out.println(data);
//    }

}

