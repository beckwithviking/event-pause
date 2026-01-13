package com.example.app.serde;

import com.example.app.model.ResumeCommand;
import org.springframework.kafka.support.serializer.JsonSerde;

public class ResumeCommandSerde extends JsonSerde<ResumeCommand> {
    public ResumeCommandSerde() {
        super(ResumeCommand.class);
    }
}
