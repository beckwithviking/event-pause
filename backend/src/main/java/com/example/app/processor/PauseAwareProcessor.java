package com.example.app.processor;

import com.example.app.model.Event;
import com.example.app.model.KeyStatus;
import com.example.app.model.ResumeCommand;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PauseAwareProcessor implements Processor<String, Object, String, Event> {

    private static final Logger log = LoggerFactory.getLogger(PauseAwareProcessor.class);
    private static final int MAX_BUFFER_SIZE = 10000; // Protection against OOM

    private ProcessorContext<String, Event> context;
    private KeyValueStore<String, List<Event>> bufferStore;
    private ReadOnlyKeyValueStore<String, KeyStatus> statusStore;

    // SCALABILITY FIX: Track keys with buffered data in memory
    // to avoid scanning the entire RocksDB in the Punctuator.
    private final Set<String> bufferedKeys = new HashSet<>();

    private final String bufferStoreName;
    private final String statusStoreName;

    public PauseAwareProcessor(String bufferStoreName, String statusStoreName) {
        this.bufferStoreName = bufferStoreName;
        this.statusStoreName = statusStoreName;
    }

    @Override
    public void init(ProcessorContext<String, Event> context) {
        this.context = context;
        this.bufferStore = context.getStateStore(bufferStoreName);
        this.statusStore = (ReadOnlyKeyValueStore<String, KeyStatus>) context.getStateStore(statusStoreName);

        // CRITICAL: On startup/restart, we must rebuild the 'bufferedKeys' set
        // from the persistent store. This runs ONCE per task assignment.
        try (KeyValueIterator<String, List<Event>> iter = bufferStore.all()) {
            while (iter.hasNext()) {
                bufferedKeys.add(iter.next().key);
            }
        }

        // SAFETY PUNCTUATOR
        // Now iterates ONLY the dirty set, not the whole DB. Efficient.
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            // Copy set to avoid ConcurrentModificationException if logic were complex
            // (Though strictly single-threaded, it's good practice)
            List<String> keysToCheck = new ArrayList<>(bufferedKeys);

            for (String key : keysToCheck) {
                KeyStatus status = getKeyStatus(key);
                if (status == KeyStatus.ACTIVE) {
                    log.info("Punctuator: Releasing stranded events for key {}", key);
                    drainBuffer(key);
                }
            }
        });
    }

    private KeyStatus getKeyStatus(String key) {
        try {
            // Determine handling based on what the store returns (raw or ValueAndTimestamp)
            // But with specific SerDe, it should likely be KeyStatus
            // We use 'Object' to be safe against ClassCastException if the store type isn't
            // perfectly clean
            Object value = statusStore.get(key);
            if (value == null)
                return KeyStatus.ACTIVE;

            if (value instanceof KeyStatus) {
                return (KeyStatus) value;
            }
            if (value instanceof org.apache.kafka.streams.state.ValueAndTimestamp) {
                return ((org.apache.kafka.streams.state.ValueAndTimestamp<KeyStatus>) value).value();
            }
            return KeyStatus.ACTIVE;
        } catch (Exception e) {
            log.error("Error retrieving status for key " + key, e);
            return KeyStatus.ACTIVE;
        }
    }

    @Override
    public void process(Record<String, Object> record) {
        String key = record.key();
        Object value = record.value();

        // HANDLE TOMBSTONES (Null values)
        if (value == null) {
            log.debug("Skipping tombstone record for key {}", key);
            return;
        }

        // === PATH A: RESUME COMMAND ===
        if (value instanceof ResumeCommand) {
            drainBuffer(key);
            return;
        }

        // === PATH B: MAIN EVENT ===
        if (value instanceof Event) {
            processEvent(key, (Event) value);
        }
    }

    private void processEvent(String key, Event newEvent) {
        KeyStatus status = getKeyStatus(key);

        // Check if we have data (Hit the in-memory set first for speed)
        boolean hasBufferedData = bufferedKeys.contains(key);

        if (status == KeyStatus.PAUSED) {
            bufferEvent(key, newEvent);
        } else if (status == KeyStatus.ACTIVE && hasBufferedData) {
            // Race condition / Lag handling
            drainBuffer(key);
            forward(key, newEvent);
        } else {
            // Happy Path
            forward(key, newEvent);
        }
    }

    private void drainBuffer(String key) {
        List<Event> events = bufferStore.get(key);
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                forward(key, e);
            }
            bufferStore.delete(key);

            // REMOVE from dirty tracking
            bufferedKeys.remove(key);
        }
    }

    private void bufferEvent(String key, Event event) {
        List<Event> events = bufferStore.get(key);
        if (events == null) {
            events = new ArrayList<>();
        }

        // Overflow Protection
        if (events.size() >= MAX_BUFFER_SIZE) {
            log.error("Buffer overflow for key {}. Dropping event.", key);
            return; // Or send to DLQ
        }

        events.add(event);
        bufferStore.put(key, events);

        // ADD to dirty tracking
        bufferedKeys.add(key);
    }

    private void forward(String key, Event event) {
        // CORRECTED: Use the original event timestamp for downstream windowing accuracy
        context.forward(new Record<>(key, event, event.timestamp()));
    }

    @Override
    public void close() {
        // RocksDB closed automatically
    }
}
