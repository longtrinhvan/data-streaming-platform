package org.akka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Builder
@Setter(AccessLevel.PUBLIC)
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    @JsonProperty("id")
    private String id;
    @JsonProperty("message")
    private String content;

}
