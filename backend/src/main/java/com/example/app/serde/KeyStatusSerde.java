package com.example.app.serde;

import com.example.app.model.KeyStatus;
import org.springframework.kafka.support.serializer.JsonSerde;

public class KeyStatusSerde extends JsonSerde<KeyStatus> {
    public KeyStatusSerde() {
        super(KeyStatus.class);
    }
}
