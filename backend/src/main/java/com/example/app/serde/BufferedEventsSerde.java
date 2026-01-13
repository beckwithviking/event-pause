package com.example.app.serde;

import com.example.app.model.Event;
import org.springframework.kafka.support.serializer.JsonSerde;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;

public class BufferedEventsSerde extends JsonSerde<List<Event>> {
    public BufferedEventsSerde() {
        super(new TypeReference<List<Event>>() {
        });
    }
}
