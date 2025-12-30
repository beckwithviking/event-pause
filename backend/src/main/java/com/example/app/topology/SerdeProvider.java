package com.example.app.topology;

import com.example.app.model.Event;
import com.example.app.model.KeyStatus;
import com.example.app.model.ResumeCommand;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SerdeProvider {

    public Serde<KeyStatus> keyStatusSerde() {
        return new JsonSerde<>(KeyStatus.class);
    }

    public Serde<Event> eventSerde() {
        return new JsonSerde<>(Event.class);
    }

    public Serde<ResumeCommand> resumeCommandSerde() {
        return new JsonSerde<>(ResumeCommand.class);
    }

    public Serde<List<Event>> bufferedEventsSerde() {
        // Need to handle generic list serialization.
        // For simplicity in this POC, we use a custom construct or default to JsonSerde
        // with type reference
        // but JsonSerde<List<Event>> works if configured correctly with JavaType or
        // TypeReference.
        // Spring's JsonSerde is robust.
        return new JsonSerde<>(new com.fasterxml.jackson.core.type.TypeReference<List<Event>>() {
        });
    }
}
