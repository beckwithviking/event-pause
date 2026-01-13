package com.example.app.serde;

import com.example.app.model.Event;
import org.springframework.kafka.support.serializer.JsonSerde;

public class EventSerde extends JsonSerde<Event> {
    public EventSerde() {
        super(Event.class);
    }
}
