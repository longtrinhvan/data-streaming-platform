package org.flink.model;

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

}
