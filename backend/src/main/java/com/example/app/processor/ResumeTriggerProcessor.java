package com.example.app.processor;

import com.example.app.model.Event;
import com.example.app.model.ResumeCommand;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class ResumeTriggerProcessor implements Processor<String, ResumeCommand, String, Event> {

    private ProcessorContext<String, Event> context;
    private KeyValueStore<String, List<Event>> bufferStore;
    private final String bufferStoreName;

    public ResumeTriggerProcessor(String bufferStoreName) {
        this.bufferStoreName = bufferStoreName;
    }

    @Override
    public void init(ProcessorContext<String, Event> context) {
        this.context = context;
        this.bufferStore = context.getStateStore(bufferStoreName);
    }

    @Override
    public void process(Record<String, ResumeCommand> record) {
        String key = record.key();
        ResumeCommand cmd = record.value();

        if (cmd != null && cmd.isResume()) {
            List<Event> events = bufferStore.get(key);
            if (events != null && !events.isEmpty()) {
                System.out.println("[RESUME-DRAIN] Key=" + key + ", BufferedEvents=" + events.size());
                for (Event e : events) {
                    System.out.println("[PROCESSED] Key=" + key + ", EventId=" + e.id() + ", Data=" + e.data() + " (resume trigger)");
                    context.forward(new Record<>(key, e, System.currentTimeMillis()));
                }
                bufferStore.delete(key);
            }
        }
    }

    @Override
    public void close() {
    }
}
