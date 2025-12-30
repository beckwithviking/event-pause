package com.example.app.processor;

import com.example.app.model.Event;
import com.example.app.model.KeyStatus;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class PauseAwareProcessor implements Processor<String, Event, String, Event> {

    private ProcessorContext<String, Event> context;
    private KeyValueStore<String, List<Event>> bufferStore;
    private ReadOnlyKeyValueStore<String, Object> statusStore;
    private final String bufferStoreName;

    public PauseAwareProcessor(String bufferStoreName) {
        this.bufferStoreName = bufferStoreName;
    }

    @Override
    public void init(ProcessorContext<String, Event> context) {
        this.context = context;
        this.bufferStore = context.getStateStore(bufferStoreName);
        // Important: GlobalKTable stores are read-only
        this.statusStore = context.getStateStore("key-status-store");
    }

    @Override
    public void process(Record<String, Event> record) {
        String key = record.key();
        Event event = record.value();

        Object value = statusStore.get(key);
        KeyStatus status = null;
        if (value instanceof org.apache.kafka.streams.state.ValueAndTimestamp) {
            status = ((org.apache.kafka.streams.state.ValueAndTimestamp<KeyStatus>) value).value();
        } else {
            status = (KeyStatus) value;
        }

        if (status == null) {
            status = KeyStatus.ACTIVE;
        }

        if (status == KeyStatus.ACTIVE) {
            drainIfNeeded(key);
            System.out.println(
                    "[PROCESSED] Key=" + key + ", EventId=" + event.id() + ", Data=" + event.data() + " (immediate)");
            context.forward(record);
        } else {
            System.out.println(
                    "[BUFFERED] Key=" + key + ", EventId=" + event.id() + ", Data=" + event.data() + " (paused)");
            buffer(key, event);
        }
    }

    private void buffer(String key, Event event) {
        List<Event> events = bufferStore.get(key);
        if (events == null) {
            events = new ArrayList<>();
        }
        events.add(event);
        bufferStore.put(key, events);
    }

    private void drainIfNeeded(String key) {
        List<Event> events = bufferStore.get(key);
        if (events != null && !events.isEmpty()) {
            System.out.println("[DRAINING] Key=" + key + ", BufferedEvents=" + events.size());
            for (Event e : events) {
                System.out.println(
                        "[PROCESSED] Key=" + key + ", EventId=" + e.id() + ", Data=" + e.data() + " (from buffer)");
                // We forward with current timestamp or original?
                // Original is in record, but here we construct new record.
                context.forward(new Record<>(key, e, System.currentTimeMillis()));
            }
            bufferStore.delete(key);
        }
    }

    @Override
    public void close() {
    }
}
