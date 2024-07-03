package org.akka.config;

import akka.actor.ActorSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Actor {

    @Bean
    public ActorSystem actorApplication() {
        return ActorSystem.create("actorApplication");
    }
}
